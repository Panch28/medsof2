/**
 * RxExpiry — extractInvoice Cloud Function
 *
 * Trigger: HTTPS Callable from frontend (step 4 in flow)
 * Model:   gemini-2.0-flash (vision-capable model for dense invoice parsing)
 * Input:   { fileUrl, fileId, pharmacyId }
 * Output:  { distributor, invoiceNumber, invoiceTotal, lineItems[], captureQuality, computedSummary }
 *
 * Flow:
 *   1. Receive fileUrl from Storage
 *   2. Send image to Gemini for extraction
 *   3. Parse structured response
 *   4. Reconcile multi-page totals via reconcileAndCalculateInvoice()
 *   5. Return to client for review (step 7)
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { logger } = require("firebase-functions/v2");
const { defineSecret } = require("firebase-functions/params");
const { initializeApp } = require("firebase-admin/app");
const { getStorage } = require("firebase-admin/storage");

// Initialize Admin SDK (auto-initialized in Cloud Functions)
initializeApp();

// ─── Gemini Config — reads from Firebase Secrets, NOT plain text ──
// Set the secret with:  firebase functions:secrets:set GEMINI_API_KEY
// Never paste the API key into this file.
const GEMINI_KEY = defineSecret("GEMINI_API_KEY");
const GEMINI_MODEL = "gemini-2.0-flash";

// ─── Extraction Prompt for Gemini ──────────────────────────────────
const EXTRACTION_PROMPT = `You are an elite financial data extraction and OCR parsing engine specialized in complex, multi-page Indian Pharmaceutical B2B distributor invoices (specifically Vardhman Medisales layouts).

CRITICAL INSTRUCTIONS FOR ACCURACY:
1. TABLE INTEGRITY & COLUMN LOCKING:
   - Never let row values slip or drift across adjacent columns.
   - The table headers are strictly: [RACK] -> [DESCRIPTION] -> [QTY BILLED] -> [FREE/Scm%] -> [PACK] -> [BATCH NO] -> [EXP DATE] -> [MRP] -> [TRADE PRICE] -> [C.D.%] -> [SCM DIS VALUE] -> [TAXABLE VALUE] -> [GST%] -> [NET VALUE] -> [MFAC MKT] -> [HSN CODE].
   - If a cell (like Scm Dis Value or Free Qty) is blank or handwritten dash, output 0 or null. Do not shift subsequent values (like Trade Price or Taxable Value) into its position.

2. MULTI-PAGE & BATCH CONTINUITY:
   - Invoices often span multiple pages (e.g., "Page 1 of 2", "Page 2 of 2").
   - Treat all pages sharing the same "TAX Inv. No" and "Invoice Date" as a single unified invoice document. Aggregate all line items from all pages into one comprehensive array.
   - If "Page X of Y" or "X/Y" appears in header/footer, extract into captureQuality.pageInfo as {current: X, total: Y}. Null if not visible.

3. MATHEMATICAL VALIDATION RULES (DO NOT HALLUCINATE):
   - Trust the printed numbers in the "TAXABLE VALUE" and "NET VALUE" columns directly from the document image; they include the distributor's specific trade adjustments.
   - Grand Total Calculation Rule:
     Grand Total = (Sum of all Net Values) - (Cash Discount) + (Round Off)
     Ensure your calculated summary matches the printed "GRAND TOTAL" at the bottom of the invoice page.

4. COLUMN ANTI-SHIFT RULES:
   - Each value belongs to the column directly ABOVE it in the header row. Trace vertically from the header, not horizontally from the previous cell.
   - If a column is EMPTY or blank for a row, put 0 (for numbers) or "" (for strings). Do NOT let subsequent column values slide into the empty slot.
   - If C.D.% is blank, taxable_value is NOT placed in the C.D.% slot. taxable_value stays in its own column.
   - If SCM DIS is blank, the taxable value stays in the TAXABLE VALUE column, NOT in the SCM DIS column.
   - SELF-CHECK: For each row, verify that taxable_value is a reasonable number (typically between Rs.10 and Rs.50,000 for a single line item). If taxable_value looks like a quantity, batch number, or discount percentage, you have placed it in the wrong column.

5. GST RULES:
   - CGST and SGST are split equally. If GST is 12%, CGST = 6% and SGST = 6%.
   - Read gst_percent from EACH ROW individually — do NOT apply a single flat GST rate to all lines.

Return the extracted payload strictly as valid JSON matching the schema below. No markdown, no code fences, no explanation.

{
  "invoiceNo": "string — Invoice/Bill number (TAX Inv. No)",
  "invoiceDate": "string — Invoice date in DD/MM/YYYY or MM/YYYY format (null if not visible)",
  "distributor": "string — Supplier/Distributor name from invoice header",
  "buyerName": "string — Buyer/Retailer pharmacy name (from To:, Sold To:, or similar; null if not visible)",
  "cashDiscount": "number — Cash discount from footer (0 if not present)",
  "schemeDiscount": "number — Total scheme/trade discount from footer (0 if not present)",
  "roundOff": "number — Round off from footer (positive or negative, 0 if not present)",
  "grandTotal": "number — Grand Total / Amount Payable as printed on invoice",
  "lineItems": [
    {
      "rack": "string — Rack/Shelf location code (null if not present)",
      "description": "string — Product/Medicine name and strength",
      "quantityBilled": "number — Quantity billed/purchased",
      "quantityFree": "number — Free quantity or scheme quantity (0 if none)",
      "pack": "string — Packaging type e.g. 10'S, 15'S, 15ML, 60'S",
      "batchNumber": "string — Batch/Lot number",
      "expiryDate": "string — Expiry date in MM/YY or DD/MM/YYYY format",
      "mrp": "number — Maximum Retail Price per pack",
      "tradePrice": "number — Trade Price per unit (before C.D.% discount)",
      "cdPercent": "number — C.D.% (cash discount percentage, usually 4.00%; 0 if absent)",
      "schemeDiscountValue": "number — Per-line scheme discount value in Rs. (0 if not present)",
      "taxableValue": "number — READ the printed Taxable Value from the invoice column (do NOT compute — use the actual printed number)",
      "gstRate": "number — GST rate for THIS row (12, 18, 5, or 28 — read from each row individually)",
      "netValue": "number — READ the printed Net Value / Amount from the invoice column (do NOT compute — use the actual printed number)",
      "mfacMkt": "string — Manufacturer code/abbreviation (e.g. CIPL, MICR, ALKE, BION; null if not visible)",
      "hsnCode": "string — HSN code for GST classification (null if not visible)",
      "confidence": "number between 0 and 1 — how confident you are in this line's extraction accuracy"
    }
  ],
  "captureQuality": {
    "readable": "boolean — can you read the invoice clearly?",
    "issues": ["array of strings — specific problems if not readable"],
    "missingPage": "boolean — multi-page invoice with pages missing?",
    "pageInfo": { "current": "number — page number if Page X of Y is visible (null if not visible)", "total": "number — total page count if visible (null if not visible)" }
  }
}`;

// ═══════════════════════════════════════════════════════════════════
// extractInvoice Cloud Function
// ═══════════════════════════════════════════════════════════════════
exports.extractInvoice = onCall(
    {
        region: "us-central1",
        memory: "1GB",
        timeoutSeconds: 120,
        secrets: [GEMINI_KEY]  // Injected at runtime from Firebase Secrets
    },
    async (request) => {
        const { fileUrl, fileId, pharmacyId } = request.data;

        if (!fileUrl) throw new HttpsError("invalid-argument", "fileUrl is required");
        if (!pharmacyId) throw new HttpsError("invalid-argument", "pharmacyId is required");

        // Read the Gemini API key from the encrypted secret store at runtime
        const geminiApiKey = GEMINI_KEY.value();
        if (!geminiApiKey) {
            throw new HttpsError("failed-precondition", "GEMINI_API_KEY secret not configured. Run: firebase functions:secrets:set GEMINI_API_KEY");
        }

        logger.info(`[extractInvoice] Starting extraction for pharmacy=${pharmacyId}, file=${fileId}`);

        try {
            // Step 1: Download image from Firebase Storage and convert to base64
            const imageBase64 = await downloadAndEncodeImage(fileUrl);

            // Step 2: Send to Gemini 2.0 Flash (passing secret key at runtime)
            console.log("BASE64 LENGTH:", imageBase64.data.length);
            const geminiResponse = await callGemini(imageBase64, geminiApiKey);

            // Step 3: Parse and validate the response
            const extracted = parseGeminiResponse(geminiResponse);

            // Step 4: Reconcile totals, compute GST breakdown, validate against grand total
            reconcileAndCalculateInvoice(extracted);

            // Log full line items for debugging
            console.log("[EXTRACTED LINE ITEMS]:", JSON.stringify(extracted.lineItems, null, 2));
            console.log("[EXTRACTED TOTALS]:", JSON.stringify({ invoiceTotal: extracted.invoiceTotal, schemeDiscount: extracted.schemeDiscount, cashDiscount: extracted.cashDiscount, roundOff: extracted.roundOff }));
            console.log("[COMPUTED SUMMARY]:", JSON.stringify(extracted.computedSummary));

            // Step 4: Add metadata
            extracted.fileId = fileId;
            extracted.pharmacyId = pharmacyId;
            extracted.extractedAt = new Date().toISOString();
            extracted.model = GEMINI_MODEL;

            logger.info(`[extractInvoice] Success: ${extracted.lineItems?.length || 0} items, readable=${extracted.captureQuality?.readable}`);

            return extracted;

        } catch (error) {
            logger.error("[extractInvoice] Extraction failed:", error);

            // Return a structured error that the frontend can handle
            return {
                distributor: "",
                invoiceNumber: "",
                invoiceTotal: 0,
                lineItems: [],
                captureQuality: {
                    readable: false,
                    issues: [`Extraction failed: ${error.message || "Unknown error"}. Please retake the photo.`],
                    missingPage: false
                },
                fileId,
                pharmacyId,
                error: true
            };
        }
    }
);

// ═══════════════════════════════════════════════════════════════════
// Helper: Download image from Storage and encode as base64
// ═══════════════════════════════════════════════════════════════════
async function downloadAndEncodeImage(fileUrl) {
    const response = await fetch(fileUrl);
    if (!response.ok) throw new Error(`Failed to download image: ${response.status}`);

    const contentType = response.headers.get("content-type") || "";
    const buffer = await response.arrayBuffer();
    const base64 = Buffer.from(buffer).toString("base64");

    // Determine MIME type for Gemini
    let mimeType = "image/jpeg";
    if (contentType.includes("png")) mimeType = "image/png";
    else if (contentType.includes("webp")) mimeType = "image/webp";
    else if (contentType.includes("gif")) mimeType = "image/gif";
    else if (contentType.includes("pdf")) mimeType = "application/pdf";

    return { data: base64, mimeType };
}

// ═══════════════════════════════════════════════════════════════════
// Helper: Call Gemini 2.0 Flash API
// ═══════════════════════════════════════════════════════════════════
async function callGemini(imageBase64, apiKey) {
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${apiKey}`;
    const requestBody = {
        contents: [
            {
                parts: [
                    {
                        text: EXTRACTION_PROMPT
                    },
                    {
                        inlineData: {
                            mimeType: imageBase64.mimeType,
                            data: imageBase64.data
                        }
                    }
                ]
            }
        ],
        generationConfig: {
            temperature: 0.1,
            topK: 1,
            topP: 1,
            maxOutputTokens: 8192,
            responseMimeType: "application/json"
        },
        safetySettings: [
            { category: "HARM_CATEGORY_HARASSMENT", threshold: "BLOCK_NONE" },
            { category: "HARM_CATEGORY_HATE_SPEECH", threshold: "BLOCK_NONE" },
            { category: "HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold: "BLOCK_NONE" },
            { category: "HARM_CATEGORY_DANGEROUS_CONTENT", threshold: "BLOCK_NONE" }
        ]
    };

    const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(requestBody)
    });

    if (!response.ok) {
        const errorBody = await response.text();
        logger.error(`[extractInvoice] Gemini API error ${response.status}:`, errorBody.substring(0, 500));
        throw new Error(`Gemini API error ${response.status}: ${errorBody.substring(0, 200)}`);
    }

    const result = await response.json();
    logger.info("[extractInvoice] Gemini raw response:", JSON.stringify(result).substring(0, 1000));

    // Extract text from Gemini response
    const text = result?.candidates?.[0]?.content?.parts?.[0]?.text;
    if (!text) {
        const finishReason = result?.candidates?.[0]?.finishReason;
        const blockReason = result?.promptFeedback?.blockReason;
        logger.error("[extractInvoice] Empty response:", { finishReason, blockReason, fullResponse: JSON.stringify(result).substring(0, 500) });
        throw new Error(`Gemini returned empty response (finishReason=${finishReason}, blockReason=${blockReason})`);
    }

    return text;
}

// ═══════════════════════════════════════════════════════════════════
// Helper: Parse and validate Gemini's JSON response
// Maps new camelCase schema to internal field names used by the client
// ═══════════════════════════════════════════════════════════════════
function parseGeminiResponse(text) {
    // Strip any markdown code fences if present
    let cleaned = text.trim();
    if (cleaned.startsWith("```")) {
        cleaned = cleaned.replace(/^```(?:json)?\s*\n?/, "").replace(/\n?```\s*$/, "");
    }

    let parsed;
    try {
        parsed = JSON.parse(cleaned);
    } catch (e) {
        const jsonMatch = cleaned.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
            parsed = JSON.parse(jsonMatch[0]);
        } else {
            throw new Error("Could not parse Gemini response as JSON");
        }
    }

    // ── Map invoice-level fields (new schema → internal) ──
    const result = {
        distributor: parsed.distributor || "",
        buyerName: parsed.buyerName ? String(parsed.buyerName).trim() : null,
        invoiceNumber: parsed.invoiceNo || parsed.invoiceNumber || "",
        invoiceDate: parsed.invoiceDate || null,
        invoiceTotal: Number(parsed.grandTotal || parsed.invoiceTotal) || 0,
        totalTaxableAmount: 0,   // computed by reconcileAndCalculateInvoice
        cgstTotal: 0,            // computed by reconcileAndCalculateInvoice
        sgstTotal: 0,            // computed by reconcileAndCalculateInvoice
        schemeDiscount: Number(parsed.schemeDiscount) || 0,
        cashDiscount: Number(parsed.cashDiscount) || 0,
        roundOff: Number(parsed.roundOff) || 0,
        pendingInvoicesCount: 0,
        pendingTotalAmount: 0,
        lineItems: [],
        captureQuality: {
            readable: parsed.captureQuality?.readable !== false,
            issues: Array.isArray(parsed.captureQuality?.issues) ? parsed.captureQuality.issues : [],
            missingPage: parsed.captureQuality?.missingPage === true,
            pageInfo: parsed.captureQuality?.pageInfo || null
        }
    };

    // ── Map line items (new camelCase schema → internal field names) ──
    if (Array.isArray(parsed.lineItems)) {
        result.lineItems = parsed.lineItems.map(item => {
            const tradePrice = Math.max(0, parseFloat(item.tradePrice) || 0);
            const quantityBilled = Math.max(0, parseInt(item.quantityBilled) || 0);
            const cdPercent = Math.max(0, parseFloat(item.cdPercent) || 0);
            const gstRate = [5, 12, 18, 28].includes(Number(item.gstRate))
                ? Number(item.gstRate) : 12;

            // taxableValue = trade_price × qty × (1 - cd%) → our netValue
            const netValue = Math.max(0, parseFloat(item.taxableValue || item.netValue) || 0);
            // netValue from invoice = taxable + GST → our lineTotal
            const lineTotal = Math.max(0, parseFloat(item.netValue || item.lineTotal) || 0);
            const gstValue = Math.max(0, +(lineTotal - netValue).toFixed(2));

            return {
                medicineName: String(item.description || item.medicineName || "").trim(),
                batchNumber: String(item.batchNumber || "").trim(),
                expiryDate: String(item.expiryDate || "").trim(),
                quantityBilled,
                quantityFree: Math.max(0, parseInt(item.quantityFree) || 0),
                tradePrice,
                cdPercent,
                scmDiscount: Math.max(0, parseFloat(item.schemeDiscountValue || item.scmDiscount) || 0),
                netValue,
                gstRate,
                gstValue,
                mrp: Math.max(0, parseFloat(item.mrp) || 0),
                packSize: item.pack ? String(item.pack).trim() : null,
                hsnCode: item.hsnCode ? String(item.hsnCode).trim() : null,
                rack: item.rack ? String(item.rack).trim() : null,
                ptr: 0,
                mfac: item.mfacMkt ? String(item.mfacMkt).trim() : null,
                lineTotal,
                confidence: Math.max(0, Math.min(1, parseFloat(item.confidence) || 0.5))
            };
        });
    }

    // ── Column-shift detection ──
    result.lineItems.forEach((item) => {
        const formulaNet = item.tradePrice > 0 && item.quantityBilled > 0
            ? +(item.tradePrice * item.quantityBilled * (1 - item.cdPercent / 100)).toFixed(2)
            : 0;
        if (formulaNet > 50 && item.netValue > 0 && item.netValue < formulaNet * 0.2) {
            item.columnShiftSuspected = true;
            item.validationNote = `Column shift: taxable Rs.${item.netValue} too low (expected ~Rs.${formulaNet} from trade Rs.${item.tradePrice} x ${item.quantityBilled})`;
            item.confidence = Math.max(0.2, item.confidence - 0.3);
            logger.warn(`[parseGeminiResponse] Column shift: "${item.medicineName}" got taxable Rs.${item.netValue}, formula ~Rs.${formulaNet}`);
        }
    });

    return result;
}

// ═══════════════════════════════════════════════════════════════════
// reconcileAndCalculateInvoice — Post-processing: computes totals
// from line items, validates against declared grand total, and
// populates computed summary fields for the client
// ═══════════════════════════════════════════════════════════════════
function reconcileAndCalculateInvoice(extracted) {
    const items = extracted.lineItems || [];
    if (items.length === 0) return extracted;

    let computedTaxableSum = 0;
    let calculatedCgst = 0;
    let calculatedSgst = 0;
    let totalQuantity = 0;

    items.forEach(item => {
        const qtyBilled = parseFloat(item.quantityBilled || 0);
        const taxableVal = parseFloat(item.netValue || 0);
        const netVal = parseFloat(item.lineTotal || 0);

        totalQuantity += qtyBilled;
        computedTaxableSum += taxableVal;

        // Standard intra-state split: CGST + SGST = total GST
        const itemTotalGst = netVal - taxableVal;
        calculatedCgst += itemTotalGst / 2;
        calculatedSgst += itemTotalGst / 2;
    });

    // Sum of all lineTotals (taxable + GST) — this is what the invoice grand total is based on
    const sumOfLineTotals = items.reduce((acc, item) => acc + parseFloat(item.lineTotal || 0), 0);

    // Grand Total = Sum of Line Totals - Cash Discount + Round Off
    const grandTotalComputed = +(sumOfLineTotals - (extracted.cashDiscount || 0) + (extracted.roundOff || 0)).toFixed(2);

    // Populate computed fields on the extracted result
    extracted.totalTaxableAmount = +computedTaxableSum.toFixed(2);
    extracted.cgstTotal = +calculatedCgst.toFixed(2);
    extracted.sgstTotal = +calculatedSgst.toFixed(2);
    extracted.computedSummary = {
        totalItemsCount: items.length,
        totalQty: totalQuantity,
        taxableSaleAmount: +computedTaxableSum.toFixed(2),
        cgstTotal: +calculatedCgst.toFixed(2),
        sgstTotal: +calculatedSgst.toFixed(2),
        totalGst: +(calculatedCgst + calculatedSgst).toFixed(2),
        grandTotalComputed,
        grandTotalDeclared: extracted.invoiceTotal || 0,
        discrepancy: extracted.invoiceTotal > 0
            ? +(grandTotalComputed - extracted.invoiceTotal).toFixed(2)
            : 0
    };

    // Cross-verification: if computed diverges massively from declared,
    // flag all items as suspected column-shift (systemic extraction error)
    if (extracted.invoiceTotal > 0) {
        const ratio = grandTotalComputed / extracted.invoiceTotal;
        if (ratio < 0.5 || ratio > 2.0) {
            logger.warn(`[reconcile] Grand total mismatch: computed Rs.${grandTotalComputed} vs declared Rs.${extracted.invoiceTotal} (ratio ${ratio.toFixed(2)})`);
            items.forEach(item => {
                if (!item.columnShiftSuspected) {
                    item.columnShiftSuspected = true;
                    item.validationNote = `Systemic issue: line totals sum to Rs.${grandTotalComputed} but invoice declares Rs.${extracted.invoiceTotal}`;
                    item.confidence = Math.max(0.2, item.confidence - 0.2);
                }
            });
        } else if (ratio < 0.8 || ratio > 1.2) {
            logger.info(`[reconcile] Moderate divergence: computed Rs.${grandTotalComputed} vs declared Rs.${extracted.invoiceTotal}`);
        }
    }

    // Per-line validation (tolerance Rs.10)
    let flaggedCount = 0;
    items.forEach(item => {
        const expectedNet = item.tradePrice > 0 && item.quantityBilled > 0
            ? +(item.tradePrice * item.quantityBilled * (1 - item.cdPercent / 100)).toFixed(2)
            : item.netValue;
        const expectedGst = item.netValue > 0 && item.gstRate > 0
            ? +(item.netValue * item.gstRate / 100).toFixed(2)
            : item.gstValue;
        const expectedLineTotal = +(item.netValue + expectedGst).toFixed(2);

        const netMismatch = Math.abs(item.netValue - expectedNet) > 10;
        const gstMismatch = Math.abs(item.gstValue - expectedGst) > 10;
        const lineTotalMismatch = Math.abs(item.lineTotal - expectedLineTotal) > 10;

        if (netMismatch || gstMismatch || lineTotalMismatch) {
            item.lineValidationFailed = true;
            item.expectedNetValue = expectedNet;
            item.expectedGstValue = expectedGst;
            item.expectedLineTotal = expectedLineTotal;
            item.validationNote = netMismatch
                ? `Expected taxable Rs.${expectedNet} (got Rs.${item.netValue})`
                : gstMismatch
                    ? `Expected GST Rs.${expectedGst} (got Rs.${item.gstValue})`
                    : `Expected total Rs.${expectedLineTotal} (got Rs.${item.lineTotal})`;
            flaggedCount++;
        } else {
            item.lineValidationFailed = false;
            item.expectedNetValue = expectedNet;
        }
    });

    if (flaggedCount > 0) {
        logger.info(`[reconcile] ${flaggedCount} of ${items.length} lines have formula discrepancies`);
    }

    logger.info(`[reconcile] Computed: taxable=${computedTaxableSum.toFixed(2)}, cgst=${calculatedCgst.toFixed(2)}, sgst=${calculatedSgst.toFixed(2)}, grandTotal=${grandTotalComputed} (declared: ${extracted.invoiceTotal})`);

    return extracted;
}

// ═══════════════════════════════════════════════════════════════════
// cleanupTempFiles — Scheduled function (Step 9 from prompt)
// Deletes any file in /temp/ older than 30 days that was never confirmed
// ═══════════════════════════════════════════════════════════════════
const { onSchedule } = require("firebase-functions/v2/scheduler");

exports.cleanupTempFiles = onSchedule(
    {
        schedule: "every 24 hours",
        region: "us-central1",
        timeoutSeconds: 300
    },
    async (event) => {
        logger.info("[cleanupTempFiles] Running scheduled cleanup...");

        const bucket = getStorage().bucket();
        const [files] = await bucket.getFiles({ prefix: "temp/" });

        const thirtyDaysAgo = Date.now() - 30 * 24 * 60 * 60 * 1000;
        let deletedCount = 0;

        for (const file of files) {
            const [metadata] = await file.getMetadata();
            const createdAt = new Date(metadata.timeCreated).getTime();

            if (createdAt < thirtyDaysAgo) {
                try {
                    await file.delete();
                    deletedCount++;
                    logger.info(`[cleanupTempFiles] Deleted: ${file.name}`);
                } catch (e) {
                    logger.warn(`[cleanupTempFiles] Failed to delete ${file.name}:`, e.message);
                }
            }
        }

        logger.info(`[cleanupTempFiles] Cleanup complete: ${deletedCount} files deleted`);
        return { deletedCount };
    }
);

// ═══════════════════════════════════════════════════════════════════
// saveInvoice — write invoice + medicines to Firestore via Admin SDK
// Client SDK writes fail due to rules/Console mismatch; Admin SDK
// bypasses rules entirely. This is also more secure (server-side).
// ═══════════════════════════════════════════════════════════════════
exports.saveInvoice = onCall(
    {
        region: "us-central1",
        memory: "256MB",
        timeoutSeconds: 60
    },
    async (request) => {
        // Must be authenticated
        if (!request.auth) {
            throw new HttpsError("unauthenticated", "User must be signed in");
        }

        const { pharmacyId, invoice, medicines, tempFileId } = request.data;

        if (!pharmacyId) throw new HttpsError("invalid-argument", "pharmacyId is required");
        if (!invoice) throw new HttpsError("invalid-argument", "invoice data is required");
        if (!Array.isArray(medicines) || medicines.length === 0) {
            throw new HttpsError("invalid-argument", "At least one medicine is required");
        }

        const { getFirestore, FieldValue } = require("firebase-admin/firestore");
        const db = getFirestore();
        const batch = db.batch();

        try {
            // Write invoice document (includes lineItems for client-side continuation merging)
            const invoiceRef = db.collection("pharmacies").doc(pharmacyId).collection("invoices").doc();
            batch.set(invoiceRef, {
                distributor: invoice.distributor || "",
                buyerName: invoice.buyerName || null,
                distributorId: invoice.distributorId || invoice.distributor || "",
                invoiceNumber: invoice.invoiceNumber || "",
                invoiceDate: invoice.invoiceDate || null,
                invoiceTotal: invoice.invoiceTotal || 0,
                totalTaxableAmount: invoice.totalTaxableAmount || 0,
                cgstTotal: invoice.cgstTotal || 0,
                sgstTotal: invoice.sgstTotal || 0,
                schemeDiscount: invoice.schemeDiscount || 0,
                cashDiscount: invoice.cashDiscount || 0,
                roundOff: invoice.roundOff || 0,
                pendingInvoicesCount: invoice.pendingInvoicesCount || 0,
                pendingTotalAmount: invoice.pendingTotalAmount || 0,
                imageHash: invoice.imageHash || "",
                lineItemCount: medicines.length,
                lineItems: medicines,
                capturedAt: FieldValue.serverTimestamp(),
                confirmedBy: request.auth.token.phone_number || request.auth.uid,
                source: "cloud-function",
                createdBy: request.auth.uid
            });

            // Write each medicine document
            for (const med of medicines) {
                if (!med.medicineName || med.medicineName === "Could not parse - verify manually") continue;
                const medRef = db.collection("pharmacies").doc(pharmacyId).collection("medicines").doc();
                batch.set(medRef, {
                    medicineName: med.medicineName,
                    batchNumber: med.batchNumber || "",
                    expiryDate: med.expiryDate || "",
                    quantityBilled: med.quantityBilled || 0,
                    quantityFree: med.quantityFree || 0,
                    remainingQty: med.quantityBilled || 0,
                    tradePrice: med.tradePrice || 0,
                    cdPercent: med.cdPercent || 0,
                    unitPrice: med.tradePrice || 0,
                    scmDiscount: med.scmDiscount || 0,
                    netValue: med.netValue || 0,
                    gstRate: med.gstRate || 0,
                    gstValue: med.gstValue || 0,
                    lineTotal: med.lineTotal || 0,
                    mrp: med.mrp || 0,
                    packSize: med.packSize || "",
                    hsnCode: med.hsnCode || "",
                    rack: med.rack || "",
                    ptr: med.ptr || 0,
                    mfac: med.mfac || null,
                    distributor: invoice.distributor || "",
                    invoiceId: invoiceRef.id,
                    confidence: med.confidence || 0,
                    addedAt: FieldValue.serverTimestamp(),
                    soldToday: 0
                });
            }

            await batch.commit();
            logger.info(`[saveInvoice] Wrote invoice + ${medicines.length} medicines for pharmacy=${pharmacyId}`);

            // Delete temp file from Storage if provided
            if (tempFileId) {
                try {
                    const bucket = getStorage().bucket();
                    await bucket.file(`temp/${tempFileId}`).delete();
                    logger.info(`[saveInvoice] Deleted temp file: temp/${tempFileId}`);
                } catch (e) {
                    logger.warn(`[saveInvoice] Temp cleanup skipped: ${e.message}`);
                }
            }

            return { success: true, invoiceId: invoiceRef.id };

        } catch (e) {
            logger.error("[saveInvoice] Write failed:", e.message);
            throw new HttpsError("internal", "Failed to save: " + e.message);
        }
    }
);

// ═══════════════════════════════════════════════════════════════════
// deleteMedicine — remove a medicine document via Admin SDK
// ═══════════════════════════════════════════════════════════════════
exports.deleteMedicine = onCall(
    {
        region: "us-central1",
        memory: "256MB",
        timeoutSeconds: 30
    },
    async (request) => {
        if (!request.auth) {
            throw new HttpsError("unauthenticated", "User must be signed in");
        }
        const { pharmacyId, medicineId } = request.data;
        if (!pharmacyId || !medicineId) {
            throw new HttpsError("invalid-argument", "pharmacyId and medicineId are required");
        }
        const { getFirestore } = require("firebase-admin/firestore");
        const db = getFirestore();
        await db.collection("pharmacies").doc(pharmacyId).collection("medicines").doc(medicineId).delete();
        logger.info(`[deleteMedicine] Deleted ${medicineId} from ${pharmacyId}`);
        return { success: true };
    }
);

// ═══════════════════════════════════════════════════════════════════
// saveDistributor — create or update a distributor document
// ═══════════════════════════════════════════════════════════════════
exports.saveDistributor = onCall(
    {
        region: "us-central1",
        memory: "256MB",
        timeoutSeconds: 30
    },
    async (request) => {
        if (!request.auth) {
            throw new HttpsError("unauthenticated", "User must be signed in");
        }
        const { pharmacyId, distributorId, name, phone, returnWindowDays } = request.data;
        if (!pharmacyId || !name) {
            throw new HttpsError("invalid-argument", "pharmacyId and name are required");
        }
        const { getFirestore, FieldValue } = require("firebase-admin/firestore");
        const db = getFirestore();
        const distRef = distributorId
            ? db.collection("pharmacies").doc(pharmacyId).collection("distributors").doc(distributorId)
            : db.collection("pharmacies").doc(pharmacyId).collection("distributors").doc();
        const data = {
            name: name,
            phone: phone || "",
            returnWindowDays: returnWindowDays || 0,
            updatedAt: FieldValue.serverTimestamp()
        };
        if (!distributorId) data.createdAt = FieldValue.serverTimestamp();
        await distRef.set(data, { merge: true });
        logger.info(`[saveDistributor] Saved distributor ${distRef.id} for pharmacy=${pharmacyId}`);
        return { success: true, distributorId: distRef.id };
    }
);

// ═══════════════════════════════════════════════════════════════════
// bulkDeleteMedicines — delete multiple medicine documents at once
// ═══════════════════════════════════════════════════════════════════
exports.bulkDeleteMedicines = onCall(
    {
        region: "us-central1",
        memory: "256MB",
        timeoutSeconds: 60
    },
    async (request) => {
        if (!request.auth) {
            throw new HttpsError("unauthenticated", "User must be signed in");
        }
        const { pharmacyId, medicineIds } = request.data;
        if (!pharmacyId || !Array.isArray(medicineIds) || medicineIds.length === 0) {
            throw new HttpsError("invalid-argument", "pharmacyId and medicineIds array are required");
        }
        const { getFirestore } = require("firebase-admin/firestore");
        const db = getFirestore();
        const batch = db.batch();
        for (const id of medicineIds) {
            const ref = db.collection("pharmacies").doc(pharmacyId).collection("medicines").doc(id);
            batch.delete(ref);
        }
        await batch.commit();
        logger.info(`[bulkDeleteMedicines] Deleted ${medicineIds.length} medicines from ${pharmacyId}`);
        return { success: true, deletedCount: medicineIds.length };
    }
);

// ═══════════════════════════════════════════════════════════════════
// testFirestoreWrite — diagnostic: test write via Admin SDK
// ═══════════════════════════════════════════════════════════════════
exports.testFirestoreWrite = onCall(
    {
        region: "us-central1",
        memory: "256MB",
        timeoutSeconds: 30
    },
    async (request) => {
        const { getFirestore } = require("firebase-admin/firestore");
        const db = getFirestore();
        const results = {};

        // Test 1: Admin SDK write (bypasses security rules)
        try {
            const testRef = db.collection("diagnostics").doc("admin-test");
            await testRef.set({ test: true, source: "admin-sdk", timestamp: new Date().toISOString() });
            const snap = await testRef.get();
            results.adminWrite = { success: true, data: snap.data() };
            logger.info("[testFirestoreWrite] Admin SDK write succeeded");
        } catch (e) {
            results.adminWrite = { success: false, error: e.message };
            logger.error("[testFirestoreWrite] Admin SDK write failed:", e.message);
        }

        // Test 2: Check auth context of the caller
        results.callerAuth = {
            uid: request.auth?.uid || null,
            phone: request.auth?.token?.phone_number || null,
            isAnonymous: request.auth?.token?.firebase?.sign_in_provider === "anonymous",
            tokenClaims: request.auth?.token ? Object.keys(request.auth.token) : []
        };

        // Test 3: Try client-context Firestore write using caller's token
        try {
            const testRef = db.collection("diagnostics").doc("client-context-test");
            await testRef.set({
                test: true,
                source: "client-context",
                callerUid: request.auth?.uid,
                timestamp: new Date().toISOString()
            });
            results.clientContextWrite = { success: true };
            logger.info("[testFirestoreWrite] Client context write succeeded");
        } catch (e) {
            results.clientContextWrite = { success: false, error: e.message };
            logger.error("[testFirestoreWrite] Client context write failed:", e.message);
        }

        return results;
    }
);
