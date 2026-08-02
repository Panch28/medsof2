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

function round2(x) {
  return Math.round(x * 100) / 100;
}

// Deterministic column-swap repair. When a line's taxable/net columns are
// swapped AND its GST value is consistent with the swap, this provably fixes
// it from arithmetic alone (no image re-read needed). Safe gate: only fires
// when every condition lines up within ₹0.50.
function repairColumnSwaps(parsed) {
  const lines = Array.isArray(parsed.lineItems) ? parsed.lineItems : [];
  let repaired = 0;
  for (const l of lines) {
    const t = Number(l.taxableValue) || 0;
    const g = Number(l.gstValue) || 0;
    const n = Number(l.netValue) || 0;
    const r = Number(l.gstRate) || 0;
    if (!(t > 0 && g > 0 && n > 0 && r > 0)) continue;
    if (Math.abs(t + g - n) <= 1) continue; // already internally consistent
    if (Math.abs(n * (1 + r / 100) - t) > 0.5) continue; // not a taxable<->net swap
    if (Math.abs((t - n) - (n * r) / 100) > 0.5) continue; // repaired GST inconsistent
    l.taxableValue = round2(n);
    l.netValue = round2(t);
    l.gstValue = round2(t - n);
    repaired++;
  }
  return repaired;
}

// Uniform-rate inference from the printed footer. When the printed CGST and
// SGST are equal and together imply a single integer GST slab for the
// discounted taxable base (saleValue − schDisc − cashDiscount), and that slab
// reproduces the printed totalGst, the whole invoice is taxed at ONE rate.
// Returns { slab, base } when confirmed, otherwise null.
function inferUniformRate(summary) {
  const saleValue = Number(summary.saleValue) || 0;
  const base =
    saleValue -
    (Number(summary.schDisc) || 0) -
    (Number(summary.cashDiscount) || 0);
  const cgst = Number(summary.totalCGST) || 0;
  const sgst = Number(summary.totalSGST) || 0;
  const igst = Number(summary.totalIGST) || 0;
  const totalGst = Number(summary.totalGst) || 0;
  if (!(base > 1 && cgst > 0 && sgst > 0 && igst === 0)) return null;
  if (Math.abs(cgst - sgst) > 1) return null;
  const pct = (cgst / base) * 200;
  const slab = Math.round(pct);
  if (![5, 12, 18, 28].includes(slab)) return null;
  if (Math.abs(pct - slab) > 0.4) return null;
  if (Math.abs(totalGst - (slab * base) / 100) > 1) return null;
  return { slab, base };
}

// Deterministic GST consistency check. Enforces per-line rate/net accuracy and
// footer totals by arithmetic, not by trusting Gemini's in-prompt self-checks.
// Returns an array of human-readable discrepancies (empty array = consistent).
function checkGstConsistency(parsed) {
  const issues = [];
  const lines = Array.isArray(parsed.lineItems) ? parsed.lineItems : [];
  const summary = parsed.invoiceSummary || {};

  // Every printed GST reference present in the footer (may be one or both).
  const refs = [];
  const totalGst = Number(summary.totalGst) || 0;
  const cst =
    (Number(summary.totalCGST) || 0) +
    (Number(summary.totalSGST) || 0) +
    (Number(summary.totalIGST) || 0);
  if (totalGst > 0) refs.push({ src: "Total GST", v: totalGst });
  if (cst > 0) refs.push({ src: "CGST+SGST+IGST", v: cst });

  if (refs.length >= 2) {
    for (let i = 0; i < refs.length; i++) {
      for (let j = i + 1; j < refs.length; j++) {
        if (Math.abs(refs[i].v - refs[j].v) > 1) {
          issues.push(`footer GST refs disagree: ${refs[i].src} ₹${refs[i].v.toFixed(2)} vs ${refs[j].src} ₹${refs[j].v.toFixed(2)}`);
        }
      }
    }
  }

  const lineGstSum = lines.reduce((s, l) => s + (Number(l.gstValue) || 0), 0);
  for (const ref of refs) {
    if (Math.abs(lineGstSum - ref.v) > 1) {
      issues.push(`line GST sum ₹${lineGstSum.toFixed(2)} != printed ${ref.src} ₹${ref.v.toFixed(2)}`);
    }
  }

  for (const l of lines) {
    const taxable = Number(l.taxableValue) || 0;
    const rate = Number(l.gstRate) || 0;
    const gv = Number(l.gstValue) || 0;
    const net = Number(l.netValue) || 0;
    if (taxable > 0 && rate > 0) {
      const expected = (taxable * rate) / 100;
      if (Math.abs(gv - expected) > 1) {
        issues.push(`"${l.medicineName || "?"}": gstValue ₹${gv.toFixed(2)} != taxable ₹${taxable.toFixed(2)} x ${rate}% = ₹${expected.toFixed(2)} (gstRate or gstValue misread)`);
      }
    }
    if (taxable > 0 && gv > 0 && net > 0) {
      const expectedNet = taxable + gv;
      if (Math.abs(net - expectedNet) > 1) {
        issues.push(`"${l.medicineName || "?"}": netValue ₹${net.toFixed(2)} != taxable ₹${taxable.toFixed(2)} + GST ₹${gv.toFixed(2)} = ₹${expectedNet.toFixed(2)} (columns misread)`);
      }
    }
  }

  // Footer formula: Grand Total = Sale − Sch Disc − Cash Disc + GST + Round Off − CN.NO
  // Back out the implied printed GST and compare it against every reference.
  const grandTotal = Number(summary.grandTotal) || 0;
  const saleValue = Number(summary.saleValue) || 0;
  if (grandTotal > 0 && saleValue > 0) {
    const implied =
      grandTotal -
      saleValue +
      (Number(summary.schDisc) || 0) +
      (Number(summary.cashDiscount) || 0) -
      (Number(summary.roundOff) || 0) +
      (Number(summary.cnNo) || 0);
    if (implied > 0) {
      const targets = refs.length ? refs : [{ src: "line GST sum", v: lineGstSum }];
      for (const ref of targets) {
        if (Math.abs(implied - ref.v) > 1) {
          issues.push(`footer formula implies GST ₹${implied.toFixed(2)} but ${ref.src} is ₹${ref.v.toFixed(2)}`);
        }
      }
    }

    // Discount check: back out the implied Cash/Sch discount from the printed
    // totals. Prevents Gemini defaulting a printed discount to ₹0.00 and then
    // failing the grand-total validation.
    const gstRef = refs.length ? refs[0].v : lineGstSum;
    const impliedDisc =
      saleValue +
      gstRef +
      (Number(summary.roundOff) || 0) -
      (Number(summary.cnNo) || 0) -
      grandTotal;
    const extractedDisc =
      (Number(summary.schDisc) || 0) + (Number(summary.cashDiscount) || 0);
    if (impliedDisc > 1 && extractedDisc < 0.5) {
      issues.push(`discount ₹${impliedDisc.toFixed(2)} appears missing (footer implies it but cash/sch disc extracted as ₹0)`);
    } else if (extractedDisc > 0.5 && Math.abs(extractedDisc - impliedDisc) > 1) {
      issues.push(`discount total ₹${extractedDisc.toFixed(2)} != implied ₹${impliedDisc.toFixed(2)} (cash/sch disc misread)`);
    }
  }

  // Taxable-total cross-check. Only on single-rate invoices (totalTaxable may be
  // a slab subtotal on multi-slab invoices, which would false-fire).
  const rates = new Set(lines.map((l) => Number(l.gstRate)).filter((r) => r > 0));
  const totalTaxable = Number(summary.totalTaxable) || 0;
  if (totalTaxable > 0 && rates.size === 1) {
    const lineTaxableSum = lines.reduce((s, l) => s + (Number(l.taxableValue) || 0), 0);
    if (Math.abs(lineTaxableSum - totalTaxable) > 1) {
      issues.push(`line taxable sum ₹${lineTaxableSum.toFixed(2)} != printed totalTaxable ₹${totalTaxable.toFixed(2)}`);
    }
  }

  // Uniform-rate enforcement. When the printed CGST/SGST imply a single integer
  // slab on the discounted base, a line reading a different rate or a taxable
  // sum that drifts from the base is provably misread — catch the self-consistent
  // misreads (e.g. a GST % column read as 5% on an all-12% invoice) that pass
  // every per-line check.
  const uniform = inferUniformRate(summary);
  if (uniform) {
    const lineTaxableSum = lines.reduce((s, l) => s + (Number(l.taxableValue) || 0), 0);
    if (Math.abs(lineTaxableSum - uniform.base) > 1) {
      issues.push(`line taxable sum ₹${lineTaxableSum.toFixed(2)} != uniform-rate taxable base ₹${uniform.base.toFixed(2)} (printed CGST/SGST imply a single ${uniform.slab}% slab)`);
    }
    for (const l of lines) {
      const rate = Number(l.gstRate) || 0;
      if (rate > 0 && Math.abs(rate - uniform.slab) > 0.5) {
        issues.push(`"${l.medicineName || "?"}": gstRate ${rate}% but printed CGST/SGST imply the ENTIRE invoice is taxed at ${uniform.slab}% (gstRate misread)`);
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

    const prompt = `You are an expert AI OCR & Accounting Parser for Indian Pharmaceutical GST Invoices (distributor formats like Vardhman Medisales / MCS software). Extract line items, metadata, invoice-level adjustments, and financial totals with 100% accounting alignment.

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

OPERATIONAL DIRECTIVES:
1. ROW-LEVEL EXTRACTION — parse EVERY visible row across these exact columns (order may vary by distributor): Rack | Description | Billed Qty | Free Qty | Pack | Batch No | Exp Date | MRP | Trade Price (unitPrice) | CD % (cdPercent) | C.D. VALUE (cdValue) | Taxable Value (taxableValue) | GST % (gstRate) | GST ₹ (gstValue) | Net Value (netValue) | Mfr/Mkt | HSN Code. Columns not present in the output schema (Rack, Pack, Mfr/Mkt, HSN Code) are read only to identify the correct line and are NOT returned.
2. COPY PRINTED VALUES, DO NOT DERIVE:
   - Copy EACH printed value independently from the image into its own field. Do NOT compute gstValue, netValue, or cdValue from arithmetic — read them as printed.
   - cdPercent is the printed CD % column (e.g. 4.00 means 4%).
   - cdValue is the printed C.D. VALUE column — the RUPPEE discount amount for that line (e.g. 35.20). COPY THE PRINTED DIGITS VERBATIM. IMPORTANT: on many Vardhman-format invoices this column literally prints ₹0.00 on EVERY line — that is genuine printed data, NOT an extraction gap. Copy it as 0. Do NOT derive a per-line discount from cdPercent × amount. Only when the column actually prints real per-line values (other distributors) read them as printed.
   - taxableValue is the printed Taxable Value column — the value AFTER any line-level discount and BEFORE GST.
   - gstRate is the printed GST % column, read PER LINE from the image. Rates commonly VARY within a single invoice (5%, 12%, 18%, 28% — e.g. CGST 2.5% + SGST 2.5% = 5%, CGST 6% + SGST 6% = 12%, CGST 9% + SGST 9% = 18%). NEVER assume all lines share one rate and NEVER default to a flat value like 12. Read each line's actual printed GST % digits.
   - netValue is the printed final line total (usually = taxableValue + gstValue).
   - GOLDEN ROW FORMULA (VERIFICATION AID ONLY — never use it to replace printed digits): Taxable Value ≈ Trade Price × Billed Qty × (1 − CD%/100). On Vardhman/MCS invoices this often does NOT hold exactly because of additional per-line adjustments. Use it ONLY as a sanity signal; if it disagrees with the printed Taxable Value, the PRINTED DIGITS are ground truth.
3. OBSCURED / HIDDEN FOOTER HANDLING:
   - If a portion of the bottom summary table is overlapped by another page or cropped, DO NOT hallucinate or invent numbers.
   - Reconstruct a hidden total mathematically ONLY from visible ground truth: (a) the visible row-level Net Values, and (b) the visible central tax distribution grid (CGST / SGST / IGST breakdown boxes or the slab tax table). Then verify with the identity: Grand Total = Total Taxable Sale Amount + Total GST + Round Off − Sch Disc − Cash Disc − CN.NO.
   - If you successfully derive a missing total this way, fill it in from that derivation and record the reconstruction in captureQuality.issues[] (e.g. "Total GST reconstructed from visible CGST/SGST grid").
   - If a total CANNOT be derived with certainty from visible data, set captureQuality.readable = false (or missingPage = true) and explain in issues[]. NEVER guess a total.
4. INVOICE SUMMARY — read the printed FOOTER summary block (below the line items) VERBATIM, field by field. This is the SINGLE source of truth for all totals:
   - saleValue: the printed "Sale Value" / "Total Sale" / "Sum of Taxable" row (the GROSS amount BEFORE discount and GST).
   - schDisc: the printed "Sch Disc." / "Scheme Discount" row (scheme discount, usually 0.00). DEDUCTED.
   - cashDiscount: the printed "Cash Disc." / "Cash Discount" / "Less: C.D." row. Copy the EXACT printed amount as a POSITIVE number (e.g. "-₹271.48" or "271.48" → 271.48). It is DEDUCTED. NEVER default it to 0 — read the actual digits; use 0 only when the invoice genuinely prints no discount row.
   - totalGst: the printed "Total GST" row (or CGST + SGST + IGST when printed separately).
   - roundOff: the printed "Round Off" amount, preserving its sign (can be negative).
   - cnNo: the printed "CN.NO." / "Credit Note" amount, if one is printed and nonzero. DEDUCTED. Use 0 if absent/empty.
   - grandTotal: the printed "Grand Total" row VERBATIM — ground truth. Do NOT compute it.
   - totalTaxable: the printed "Taxable Sale Amount" subtotal(s) for the GST slabs (e.g. 10136.87 for 6% + 683.38 for 9%).
   - totalCGST / totalSGST / totalIGST: the printed CGST / SGST / IGST amounts (usually equal, e.g. 608.21 / 608.21).
5. The printed Grand Total is ground truth. It should satisfy:
   Grand Total = Sale Value − Sch Disc − Cash Disc + Total GST + Round Off − CN.NO
6. If multiple images are provided, they represent consecutive pages of the SAME invoice. Combine all line items. The footer totals are usually on the last page.
7. confidence values are 0.0 to 1.0 per field. If the image is blurry or unreadable, set captureQuality.readable = false and list reasons in issues[].
8. All amounts in INR as plain numbers (no ₹ symbol), typed as numbers, never strings. gstRate and cdPercent are percentages (e.g. 12, 4.00).
9. Return ONLY valid JSON — no markdown, no explanation.
10. SELF-CHECK before returning (this is mandatory):
    a. sum of ALL line gstValue values should approximately equal the printed Total GST (totalGst) — within ₹1.
    b. Verify the footer formula: saleValue − schDisc − cashDiscount + totalGst + roundOff − cnNo should approximately equal grandTotal (within ₹1). If it does not, you MISREAD a footer field — re-scan the footer summary block and CORRECT the specific misread field from the printed digits (pay special attention to the "Cash Disc." / "Sch Disc." rows — never read a printed discount as ₹0.00). Do NOT change line items to fake a match.
    c. PER-LINE GST RATE check: for EVERY line, gstValue should be within ₹1 of (taxableValue × gstRate / 100). If a line's gstRate disagrees with its gstValue, you MISREAD the GST % column — re-scan that line and correct gstRate from the printed digits.
    d. RATE-SLAB check: the DISTINCT gstRate values across lines must match the GST % slabs shown in the footer tax table. A footer row "CGST 9% + SGST 9%" means those lines are taxed at 18%; "CGST 2.5% + SGST 2.5%" means 5%. If your lines show ONLY ONE rate but the footer shows TWO or MORE slabs, you FLATTENED the GST % column — re-scan EVERY line and read each actual per-line rate.
    e. PER-LINE NET check: for EVERY line, netValue should be within ₹1 of (taxableValue + gstValue). If a line's netValue disagrees with taxableValue + gstValue, you likely SWAPPED the Taxable and Net columns — re-scan that line and read the two printed amounts back into their correct columns.
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
    let gstAttempts = 0;
    for (let attempt = 0; attempt <= 4; attempt++) {
      repairColumnSwaps(parsed);
      gstIssues = checkGstConsistency(parsed);
      gstAttempts = attempt + 1;
      if (gstIssues.length === 0) break;
      if (attempt === 4) break; // correction budget exhausted; final state already checked

      logger.warn(`[extractInvoice] GST check failed (attempt ${attempt + 1}): ${gstIssues.join(" | ")}`);
      const lineGstSum = (parsed.lineItems || []).reduce((s, l) => s + (Number(l.gstValue) || 0), 0);
      const refsInfo = [];
      const totalGst = Number(parsed.invoiceSummary?.totalGst) || 0;
      const cst =
        (Number(parsed.invoiceSummary?.totalCGST) || 0) +
        (Number(parsed.invoiceSummary?.totalSGST) || 0) +
        (Number(parsed.invoiceSummary?.totalIGST) || 0);
      if (totalGst > 0) refsInfo.push(`Total GST ₹${totalGst.toFixed(2)}`);
      if (cst > 0) refsInfo.push(`CGST+SGST+IGST ₹${cst.toFixed(2)}`);
      const printedRef = refsInfo.length ? refsInfo.join(", ") : "not extracted";
      const uniform = inferUniformRate(parsed.invoiceSummary || {});
      const uniformHint = uniform
        ? `\nIMPORTANT: the printed CGST ₹${(Number(parsed.invoiceSummary?.totalCGST) || 0).toFixed(2)} and SGST ₹${(Number(parsed.invoiceSummary?.totalSGST) || 0).toFixed(2)} are equal and together imply the ENTIRE invoice is taxed at a single ${uniform.slab}% rate (CGST = SGST = ${uniform.slab / 2}% of the discounted taxable base ₹${uniform.base.toFixed(2)}), and that reproduces the printed Total GST ₹${(Number(parsed.invoiceSummary?.totalGst) || 0).toFixed(2)}. Therefore EVERY line must have gstRate = ${uniform.slab}; any line reporting a different rate (e.g. 5) is a misread of that line's GST % column, and the sum of ALL line taxableValue must equal ₹${uniform.base.toFixed(2)}.`
        : "";
      const correction = `You previously returned this JSON for the invoice images:\n\n${JSON.stringify(parsed)}\n\nThe following deterministic GST checks FAILED against the printed figures:\n- ${gstIssues.join("\n- ")}\n\nThe printed footer GST totals are GROUND TRUTH and equal: ${printedRef}. Your line GST sum is ₹${lineGstSum.toFixed(2)} — it must equal the printed total within ₹1.${uniformHint}\nRe-read the ACTUAL printed digits of EVERY line: the GST % column, the GST ₹ column, and the Taxable/Net columns. If a line is internally broken (taxableValue + gstValue != netValue) the Taxable and Net columns were probably SWAPPED — swap them back from the printed digits.\nCorrect any of gstRate, gstValue, taxableValue, netValue (only to repair a column swap or rate misread) and the footer totalGst / totalCGST / totalSGST / totalIGST / saleValue / grandTotal / totalTaxable / schDisc / cashDiscount / roundOff / cnNo if misread, so that:\n1. sum of ALL line gstValue == printed Total GST (within ₹1)\n2. for EVERY line: gstValue == taxableValue x gstRate / 100 (within ₹1)\n3. for EVERY line: netValue == taxableValue + gstValue (within ₹1)\n4. the distinct gstRate values match the GST % slabs in the footer tax table.\n5. if a "Cash Disc." or "Sch Disc." row is printed in the footer, the discount must be captured — it should equal saleValue + Total GST + Round Off − CN.NO − grandTotal (the implied discount). NEVER leave a printed discount as ₹0.00; re-read the printed discount row digits.\n6. if totalTaxable is printed, sum of ALL line taxableValue == printed totalTaxable (within ₹1).\nRe-read the ACTUAL printed digits of every line's GST % column — NEVER assume or default a rate such as 12.\nDo NOT change medicineName, batchNumber, expiryDate, quantities, unitPrice, cdPercent, cdValue.\nReturn ONLY the corrected JSON with the SAME schema — no markdown, no explanation.`;
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

    logger.info(`[extractInvoice] done invoice=${parsed.invoiceNumber || "?"} items=${(parsed.lineItems || []).length} gstPass=${gstIssues.length === 0}`);
    return {
      ...parsed,
      gstCheck: { pass: gstIssues.length === 0, attempts: gstAttempts, issues: gstIssues },
      rawGeminiResponse: geminiResponse,
    };
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
        gstCheck: invoice.gstCheck || {},
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
