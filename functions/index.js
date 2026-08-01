/**
 * RxExpiry Cloud Functions
 * - extractInvoice: Synchronous HTTPS callable. Accepts a Storage path,
 *   downloads the file, sends to Gemini gemini-2.0-flash, returns structured data.
 * - scheduledCleanup: Runs daily, deletes raw uploads older than 30 days.
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { defineSecret } = require("firebase-functions/params");
const { logger } = require("firebase-functions/v2");
const { initializeApp } = require("firebase-admin/app");
const { getStorage } = require("firebase-admin/storage");
const { getFirestore, FieldValue } = require("firebase-admin/firestore");
const { GoogleGenerativeAI } = require("@google/generative-ai");

initializeApp();

const GEMINI_API_KEY = defineSecret("GEMINI_API_KEY");

// Deterministic GST consistency check. Enforces per-line rate accuracy by
// arithmetic, not by trusting Gemini's in-prompt self-checks. Returns an array
// of human-readable discrepancies (empty array = consistent).
function checkGstConsistency(parsed) {
  const issues = [];
  const lines = Array.isArray(parsed.lineItems) ? parsed.lineItems : [];
  const summary = parsed.invoiceSummary || {};

  const printedGst = summary.totalGst ?? ((summary.totalCGST || 0) + (summary.totalSGST || 0) + (summary.totalIGST || 0));
  const lineGstSum = lines.reduce((s, l) => s + (Number(l.gstValue) || 0), 0);

  if (printedGst && Math.abs(lineGstSum - printedGst) > 1) {
    issues.push(`line GST sum ₹${lineGstSum.toFixed(2)} != printed Total GST ₹${printedGst.toFixed(2)}`);
  }

  for (const l of lines) {
    const taxable = Number(l.taxableValue) || 0;
    const rate = Number(l.gstRate) || 0;
    const gv = Number(l.gstValue) || 0;
    if (taxable > 0 && rate > 0) {
      const expected = (taxable * rate) / 100;
      if (Math.abs(gv - expected) > 1) {
        issues.push(`"${l.medicineName || "?"}": gstValue ₹${gv.toFixed(2)} != taxable ₹${taxable.toFixed(2)} x ${rate}% = ₹${expected.toFixed(2)} (gstRate or gstValue misread)`);
      }
    }
  }
  return issues;
}

// ─── extractInvoice ───────────────────────────────────────────────────────────

exports.extractInvoice = onCall(
  {
    timeoutSeconds: 120,
    memory: "512MiB",
    region: "us-central1",
    secrets: ["GEMINI_API_KEY"],
  },
  async (request) => {
    // Auth check
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }

    let paths = [];
    if (request.data.storagePaths && Array.isArray(request.data.storagePaths)) {
      paths = request.data.storagePaths;
    } else if (request.data.storagePath) {
      paths = [request.data.storagePath];
    }

    const { pharmacyId } = request.data;
    if (paths.length === 0 || !pharmacyId) {
      throw new HttpsError("invalid-argument", "storagePath/storagePaths and pharmacyId are required.");
    }

    // Verify all paths belong to this pharmacy (security check)
    for (const p of paths) {
      if (!p.startsWith(`invoices/${pharmacyId}/`)) {
        throw new HttpsError("permission-denied", "Invalid storage path: " + p);
      }
    }

    // Download all files from Storage into Buffers
    const bucket = getStorage().bucket();
    let imageParts;
    try {
      imageParts = await Promise.all(
        paths.map(async (p) => {
          const file = bucket.file(p);
          const [metadata] = await file.getMetadata();
          const [contents] = await file.download();
          return {
            inlineData: {
              data: contents.toString("base64"),
              mimeType: metadata.contentType || "image/jpeg",
            },
          };
        })
      );
    } catch (err) {
      throw new HttpsError("not-found", "Error downloading files from storage: " + err.message);
    }

    // Call Gemini
    const apiKey = GEMINI_API_KEY.value();
    if (!apiKey) {
      throw new HttpsError("internal", "GEMINI_API_KEY secret not configured.");
    }

    const genAI = new GoogleGenerativeAI(apiKey);
    const model = genAI.getGenerativeModel({ model: "gemini-3.5-flash-lite" });

    const prompt = `You are an advanced, high-precision OCR and invoice parsing engine specialized in Indian Pharmaceutical GST Invoices. Extract line items, metadata, invoice-level adjustments, and financial totals with 100% accounting alignment.

Return ONLY valid JSON matching this exact schema — no markdown, no explanation:

{
  "distributor": "string",
  "invoiceNumber": "string",
  "invoiceDate": "string (DD/MM/YYYY or as printed)",
  "invoiceTotal": number,
  "captureQuality": {
    "readable": boolean,
    "issues": ["string"],
    "missingPage": boolean
  },
  "invoiceSummary": {
    "saleValue": number,
    "schDisc": number,
    "cashDiscount": number,
    "totalGst": number,
    "roundOff": number,
    "cnNo": number,
    "grandTotal": number,
    "totalTaxable": number,
    "totalCGST": number,
    "totalSGST": number,
    "totalIGST": number
  },
  "lineItems": [
    {
      "medicineName": "string",
      "batchNumber": "string",
      "expiryDate": "string (MM/YYYY or as printed)",
      "quantityBilled": number,
      "quantityFree": number,
      "unitPrice": number,
      "taxableValue": number,
      "cdPercent": number,
      "cdValue": number,
      "netValue": number,
      "gstRate": number,
      "gstValue": number,
      "confidence": {
        "medicineName": number,
        "batchNumber": number,
        "expiryDate": number,
        "quantityBilled": number,
        "quantityFree": number,
        "unitPrice": number,
        "taxableValue": number,
        "cdPercent": number,
        "cdValue": number,
        "netValue": number,
        "gstRate": number,
        "gstValue": number
      }
    }
  ]
}

CRITICAL — COPY PRINTED VALUES, DO NOT DERIVE:
1. PER LINE ITEM — typical column layout (order may vary by distributor):
   MRP | Trade Price (unitPrice) | CD % (cdPercent) | C.D. VALUE (cdValue) | Taxable Value (taxableValue) | GST % (gstRate) | GST ₹ (gstValue) | Net Value (netValue).
   Copy EACH printed value independently from the image into its own field. Do NOT compute gstValue, netValue, or cdValue from arithmetic — read them as printed.
   - cdPercent is the printed CD % column (e.g. 4.00 means 4%).
   - cdValue is the printed C.D. VALUE column — the RUPPEE discount amount for that line (e.g. 35.20). COPY THE PRINTED DIGITS VERBATIM. IMPORTANT: on many Vardhman-format invoices this column literally prints ₹0.00 on EVERY line — that is genuine printed data, NOT an extraction gap. Copy it as 0. Do NOT derive a per-line discount from cdPercent × amount. Only when the column actually prints real per-line values (other distributors) read them as printed.
   - taxableValue is the printed Taxable Value column — the value AFTER any line-level discount and BEFORE GST.
   - gstRate is the printed GST % column, read PER LINE from the image. Rates commonly VARY within a single invoice (5%, 12%, 18%, 28% — e.g. CGST 2.5% + SGST 2.5% = 5%, CGST 6% + SGST 6% = 12%, CGST 9% + SGST 9% = 18%). NEVER assume all lines share one rate and NEVER default to a flat value like 12. Read each line's actual printed GST % digits.
   - netValue is the printed final line total (usually = taxableValue + gstValue).
2. INVOICE SUMMARY — read the printed FOOTER summary block (below the line items) VERBATIM, field by field. This is the SINGLE source of truth for all totals:
   - saleValue: the printed "Sale Value" / "Total Sale" / "Sum of Taxable" row (the GROSS amount BEFORE discount and GST).
   - schDisc: the printed "Sch Disc." / "Scheme Discount" row (scheme discount, usually 0.00). DEDUCTED.
   - cashDiscount: the printed "Cash Disc." / "Cash Discount" / "Less: C.D." row. Copy the EXACT printed amount as a POSITIVE number (e.g. "-₹271.48" or "271.48" → 271.48). It is DEDUCTED. NEVER default it to 0 — read the actual digits; use 0 only when the invoice genuinely prints no discount row.
   - totalGst: the printed "Total GST" row (or CGST + SGST + IGST when printed separately).
   - roundOff: the printed "Round Off" amount, preserving its sign (can be negative).
   - cnNo: the printed "CN.NO." / "Credit Note" amount, if one is printed and nonzero. DEDUCTED. Use 0 if absent/empty.
   - grandTotal: the printed "Grand Total" row VERBATIM — ground truth. Do NOT compute it.
   - totalTaxable: the printed "Taxable Sale Amount" subtotal(s) for the GST slabs (e.g. 10136.87 for 6% + 683.38 for 9%).
   - totalCGST / totalSGST / totalIGST: the printed CGST / SGST / IGST amounts (usually equal, e.g. 608.21 / 608.21).
3. The printed Grand Total is ground truth. It should satisfy:
   Grand Total = Sale Value − Sch Disc − Cash Disc + Total GST + Round Off − CN.NO
4. If multiple images are provided, they represent consecutive pages of the SAME invoice. Combine all line items. The footer totals are usually on the last page.
5. confidence values are 0.0 to 1.0 per field. If the image is blurry or unreadable, set captureQuality.readable = false and list reasons in issues[].
6. All amounts in INR as plain numbers (no ₹ symbol). gstRate and cdPercent are percentages (e.g. 12, 4.00).
7. Return ONLY valid JSON — no markdown, no explanation.
8. SELF-CHECK before returning (this is mandatory):
   a. sum of ALL line gstValue values should approximately equal the printed Total GST (totalGst) — within ₹1.
   b. Verify the footer formula: saleValue − schDisc − cashDiscount + totalGst + roundOff − cnNo should approximately equal grandTotal (within ₹1). If it does not, you MISREAD a footer field — re-scan the footer summary block and CORRECT the specific misread field from the printed digits. Do NOT change line items to fake a match.
   c. PER-LINE GST RATE check: for EVERY line, gstValue should be within ₹1 of (taxableValue × gstRate / 100). If a line's gstRate disagrees with its gstValue, you MISREAD the GST % column — re-scan that line and correct gstRate from the printed digits.
   d. RATE-SLAB check: the DISTINCT gstRate values across lines must match the GST % slabs shown in the footer tax table. A footer row "CGST 9% + SGST 9%" means those lines are taxed at 18%; "CGST 2.5% + SGST 2.5%" means 5%. If your lines show ONLY ONE rate but the footer shows TWO or MORE slabs, you FLATTENED the GST % column — re-scan EVERY line and read each actual per-line rate.
   If the footer is genuinely unreadable or off-page, set captureQuality.readable = false (or missingPage = true) and explain in issues[].`;

    let geminiResponse;
    try {
      const result = await model.generateContent([prompt, ...imageParts]);
      geminiResponse = result.response.text();
    } catch (err) {
      throw new HttpsError("internal", "Gemini API error: " + err.message);
    }

    // Parse JSON from Gemini response
    let parsed;
    try {
      // Strip any accidental markdown fences
      const cleaned = geminiResponse
        .replace(/^```json\s*/i, "")
        .replace(/^```\s*/i, "")
        .replace(/```\s*$/i, "")
        .trim();
      parsed = JSON.parse(cleaned);
    } catch (err) {
      throw new HttpsError(
        "internal",
        "Failed to parse Gemini JSON response: " + geminiResponse.substring(0, 300)
      );
    }

    // Deterministic GST enforcement: validate the arithmetic ourselves and, if it
    // fails, send Gemini a corrective turn telling it exactly which lines are
    // inconsistent. Gemini's in-prompt self-checks are advisory; this backstop is
    // what makes mixed-rate invoices (5/12/18 etc.) reliable.
    let gstIssues = [];
    for (let attempt = 0; attempt <= 2; attempt++) {
      gstIssues = checkGstConsistency(parsed);
      if (gstIssues.length === 0) break;

      logger.warn(`[extractInvoice] GST check failed (attempt ${attempt + 1}): ${gstIssues.join(" | ")}`);
      const correction = `You previously returned this JSON for the invoice images:\n\n${JSON.stringify(parsed)}\n\nThe following GST checks FAILED against the printed figures:\n- ${gstIssues.join("\n- ")}\n\nThe printed per-line GST % column and the printed footer GST totals are GROUND TRUTH. Correct ONLY the gstRate / gstValue fields and the footer totalGst / totalCGST / totalSGST / totalIGST if misread, so that:\n1. sum of ALL line gstValue == printed Total GST (within ₹1)\n2. for EVERY line: gstValue == taxableValue x gstRate / 100 (within ₹1)\n3. the distinct gstRate values match the GST % slabs in the footer tax table.\nRe-read the ACTUAL printed digits of every line's GST % column — NEVER assume or default a rate such as 12.\nDo NOT change medicineName, batchNumber, expiryDate, quantities, prices, cdPercent, cdValue, or the Cash Discount.\nReturn ONLY the corrected JSON with the SAME schema — no markdown, no explanation.`;
      try {
        const result = await model.generateContent([correction, ...imageParts]);
        const text = result.response.text();
        const cleaned = text
          .replace(/^```json\s*/i, "")
          .replace(/^```\s*/i, "")
          .replace(/```\s*$/i, "")
          .trim();
        parsed = JSON.parse(cleaned);
      } catch (err) {
        logger.error("[extractInvoice] GST correction attempt failed: " + err.message);
        break;
      }
    }
    if (gstIssues.length > 0) {
      logger.warn(`[extractInvoice] GST still inconsistent after retries: ${gstIssues.join(" | ")}`);
    }

    logger.info(`[extractInvoice] done invoice=${parsed.invoiceNumber || "?"} items=${(parsed.lineItems || []).length}`);
    return { ...parsed, rawGeminiResponse: geminiResponse };
  }
);

// ─── saveInvoice ───────────────────────────────────────────────────────────────
// Client SDK writes fail due to rules/Console mismatch; Admin SDK
// bypasses rules entirely. This is also more secure (server-side).

exports.saveInvoice = onCall(
  {
    region: "us-central1",
    memory: "256MiB",
    timeoutSeconds: 60,
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }

    const { pharmacyId, invoice, lineItems, confirmedBy } = request.data;

    if (!pharmacyId) throw new HttpsError("invalid-argument", "pharmacyId is required");
    if (!invoice) throw new HttpsError("invalid-argument", "invoice data is required");
    if (!Array.isArray(lineItems) || lineItems.length === 0) {
      throw new HttpsError("invalid-argument", "At least one line item is required");
    }

    const db = getFirestore();

    try {
      const invoiceRef = await db.collection("pharmacies").doc(pharmacyId).collection("invoices").add({
        distributor: invoice.distributor || "",
        invoiceNumber: invoice.invoiceNumber || "",
        invoiceDate: invoice.invoiceDate || "",
        invoiceTotal: invoice.invoiceTotal || 0,
        invoiceSummary: invoice.invoiceSummary || {},
        lineItems,
        captureQuality: invoice.captureQuality || {},
        rawGeminiResponse: invoice.rawGeminiResponse || "",
        confirmedBy: confirmedBy || request.auth.uid,
        confirmedAt: FieldValue.serverTimestamp(),
        createdAt: FieldValue.serverTimestamp(),
      });

      const batch = db.batch();
      for (const item of lineItems) {
        if (!item.medicineName) continue;
        const medRef = db.collection("pharmacies").doc(pharmacyId).collection("medicines").doc();
        batch.set(medRef, {
          medicineName: item.medicineName,
          batchNumber: item.batchNumber || "",
          expiryDate: item.expiryDate || "",
          quantityBilled: item.quantityBilled || 0,
          quantityFree: item.quantityFree || 0,
          unitPrice: item.unitPrice || 0,
          cdPercent: item.cdPercent || 0,
          taxableValue: item.taxableValue || 0,
          cdValue: item.cdValue || 0,
          netValue: item.netValue || 0,
          gstRate: item.gstRate || 0,
          gstValue: item.gstValue || 0,
          remainingQty: (item.quantityBilled || 0) + (item.quantityFree || 0),
          distributor: invoice.distributor || "",
          invoiceId: invoiceRef.id,
          pharmacyId,
          createdAt: FieldValue.serverTimestamp(),
          updatedAt: FieldValue.serverTimestamp(),
        });
      }
      await batch.commit();

      logger.info(`[saveInvoice] Saved invoice ${invoiceRef.id} with ${lineItems.length} items for pharmacy=${pharmacyId}`);

      return { success: true, invoiceId: invoiceRef.id };
    } catch (err) {
      logger.error("[saveInvoice] Write failed:", err.message);
      throw new HttpsError("internal", "Failed to save: " + err.message);
    }
  }
);

// ─── scheduledCleanup ─────────────────────────────────────────────────────────

exports.scheduledCleanup = onSchedule(
  {
    schedule: "every 24 hours",
    region: "us-central1",
    timeoutSeconds: 300,
  },
  async () => {
    const bucket = getStorage().bucket();
    const db = getFirestore();
    const cutoff = Date.now() - 30 * 24 * 60 * 60 * 1000; // 30 days

    try {
      // List all files under invoices/
      const [files] = await bucket.getFiles({ prefix: "invoices/" });

      let deleted = 0;
      for (const file of files) {
        const [meta] = await file.getMetadata();
        const created = new Date(meta.timeCreated).getTime();
        if (created < cutoff) {
          await file.delete();
          deleted++;
          console.log(`Cleanup: deleted stale file ${file.name}`);
        }
      }
      // Strip rawGeminiResponse debug field from invoices older than 14 days (TTL)
      try {
        const ttlCutoff = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000);
        const snap = await db.collectionGroup("invoices").where("createdAt", "<=", ttlCutoff).limit(500).get();
        let stripped = 0;
        for (const doc of snap.docs) {
          await doc.ref.update({ rawGeminiResponse: FieldValue.delete() });
          stripped++;
        }
        console.log(`Cleanup: stripped rawGeminiResponse from ${stripped} old invoice(s).`);
      } catch (stripErr) {
        console.error("Cleanup strip error:", stripErr.message);
      }

      console.log(`Cleanup complete. Deleted ${deleted} stale file(s).`);
    } catch (err) {
      console.error("Cleanup error:", err);
    }
  }
);
