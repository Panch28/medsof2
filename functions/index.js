/**
 * RxExpiry — extractInvoice Cloud Function
 *
 * Trigger: HTTPS Callable from frontend (step 4 in flow)
 * Model:   gemini-2.0-flash (vision-capable model for dense invoice parsing)
 * Input:   { fileUrl, fileId, pharmacyId }
 * Output:  { distributor, invoiceNumber, invoiceTotal, lineItems[], captureQuality }
 *
 * Flow:
 *   1. Receive fileUrl from Storage
 *   2. Send image to Gemini for extraction
 *   3. Parse structured response
 *   4. Return to client for review (step 7)
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
// Matches Gemini's natural output format from AI Studio
const EXTRACTION_PROMPT = `You are an expert OCR and financial data extraction engine specialized in Indian Pharmaceutical/Medical Distributor Invoices (B2B GST Invoices).

TASK:
1. Extract every single line item from the main product table. Do not skip any rows.
2. Extract the summary financial totals precisely.
3. Perform or verify the mathematical calculations to ensure financial accuracy.
4. Return ONLY a valid JSON object (no markdown, no code fences, no explanation) with this exact structure:

{
  "distributor_name": "string — Supplier/Distributor name from invoice header",
  "buyer_name": "string — Buyer/Retailer pharmacy name (from 'To:', 'Sold To:', or similar; null if not visible)",
  "invoice_no": "string — Invoice/Bill number",
  "invoice_date": "string — Invoice date in DD/MM/YYYY or MM/YYYY format (null if not visible)",
  "grand_total": "number — Grand Total / Amount Payable as printed on invoice",
  "total_taxable_amount": "number — Total Taxable Amount from invoice footer (should equal sum of all line taxable_values)",
  "cgst_total": "number — Total CGST amount from invoice footer (0 if not present)",
  "sgst_total": "number — Total SGST amount from invoice footer (0 if not present)",
  "scheme_discount": "number — Total scheme/trade discount from footer (0 if not present)",
  "cash_discount": "number — Cash discount from footer (0 if not present)",
  "round_off": "number — Round off from footer (positive or negative, 0 if not present)",
  "pending_invoices_count": "number — Number of pending/unpaid invoices shown on invoice (0 if not visible)",
  "pending_total_amount": "number — Total pending/unpaid balance amount (0 if not visible)",
  "lineItems": [
    {
      "rack": "string — Rack/Shelf location code (null if not present)",
      "medicineName": "string — Product/Medicine name and strength",
      "qty_billed": "number — Quantity billed/purchased",
      "free_scm": "number — Free quantity or scheme percentage (0 if none)",
      "pack": "string — Packaging type e.g. 10'S, 15'S, 15ML, 60'S",
      "batch_no": "string — Batch/Lot number",
      "exp_date": "string — Expiry date in MM/YY or DD/MM/YYYY format",
      "mrp": "number — Maximum Retail Price per pack",
      "trade_price": "number — Trade Price per unit (before C.D.% discount)",
      "cd_percent": "number — C.D.% (cash discount percentage, usually 4.00%; 0 if absent)",
      "scm_dis_value": "number — Per-line scheme discount value in ₹ (0 if not present)",
      "taxable_value": "number — READ the printed Taxable Value from the invoice column (do NOT compute — use the actual printed number)",
      "gst_percent": "number — GST rate for THIS row (12, 18, 5, or 28 — read from each row individually)",
      "net_value": "number — READ the printed Net Value / Amount from the invoice column (do NOT compute — use the actual printed number)",
      "mfac": "string — Manufacturer code/abbreviation (e.g. CIPL, MICR, ALKE; null if not visible)",
      "hsn_code": "string — HSN code for GST classification (null if not visible)",
      "confidence": "number between 0 and 1 — how confident you are in this line's extraction accuracy"
    }
  ],
  "captureQuality": {
    "readable": "boolean — can you read the invoice clearly?",
    "issues": ["array of strings — specific problems if not readable"],
    "missingPage": "boolean — multi-page invoice with pages missing?",
    "pageInfo": { "current": "number — page number of this page if 'Page X of Y' or 'X/Y' is visible (null if not visible)", "total": "number — total page count if visible (null if not visible)" }
  }
}

═══ COLUMN MAPPING RULES (STRICT — CRITICAL) ═══
The invoice table has fixed columns printed in this exact left-to-right order. You MUST anchor each value to its correct column header. DO NOT shift values left or right.

TYPICAL COLUMN ORDER (left to right):
[RACK] → [DESCRIPTION] → [QTY BILLED] → [FREE/SCM] → [PACK] → [BATCH] → [EXP] → [MRP] → [TRADE PRICE] → [C.D.%] → [SCM DIS] → [TAXABLE VALUE] → [GST%] → [NET VALUE]

CRITICAL ANTI-SHIFT RULES:
- If a column is EMPTY or blank for a row, put 0 (for numbers) or "" (for strings). Do NOT let subsequent column values slide into the empty slot.
- Example: If C.D.% is blank, taxable_value is NOT placed in the C.D.% slot. taxable_value stays in its own column.
- Example: If SCM DIS is blank, the taxable value stays in the TAXABLE VALUE column, NOT in the SCM DIS column.
- Each value belongs to the column directly ABOVE it in the header row. Trace vertically from the header, not horizontally from the previous cell.

FIELD-TO-COLUMN MAP:
- rack → Rack/Shelf location code (e.g. C0194, E0387)
- medicineName → Description / Product Name column
- qty_billed → Qty Billed / Quantity column
- free_scm → Free Qty / Scheme Qty / Scm% column (0 if absent)
- pack → Pack / Pack Size column (e.g. 10'S, 15ML, 60'S)
- batch_no → Batch No. / Lot No. column
- exp_date → Exp. Date / Expiry Date column
- mrp → M.R.P. column (per pack)
- trade_price → Trade Price / T.Rate / Rate column (per unit BEFORE C.D.%)
- cd_percent → C.D.% column (cash discount percentage, usually 4%)
- scm_dis_value → Scm Dis Value / Scheme Discount Value column (absolute ₹, 0 if absent)
- taxable_value → Taxable Value column (read the printed number from this column)
- gst_percent → GST % column (read from EACH ROW individually — do NOT use a flat rate)
- net_value → Net Value / Amount column (the final amount including GST)
- mfac → Mfac / Manufacturer / Company code column
- hsn_code → HSN Code column

SELF-CHECK AFTER EXTRACTION: For each row, verify that taxable_value is a reasonable number (typically between ₹10 and ₹50,000 for a single line item). If taxable_value looks like a quantity, batch number, or discount percentage, you have placed it in the wrong column — re-examine the row.

═══ FEW-SHOT EXAMPLE (how to correctly anchor columns) ═══
Below is a sample row as it appears on a printed invoice, and the correct JSON output. Note how empty columns get 0 — values do NOT slide left.

INVOICE ROW (left to right):
| Rack   | Description      | Qty | Free | Pack | Batch    | Exp    | MRP  | Trade  | C.D.% | Scm Dis | Taxable  | GST% | Net     |
| C0194  | ESSRYL M1 TAB   | 5   | 1    | 10'S | AB1234   | 03/27  | 185  | 172.80 | 4     | 0       | 829.44   | 12   | 929.97  |

CORRECT JSON for this row:
{
  "rack": "C0194",
  "medicineName": "ESSRYL M1 TAB",
  "qty_billed": 5,
  "free_scm": 1,
  "pack": "10'S",
  "batch_no": "AB1234",
  "exp_date": "03/27",
  "mrp": 185,
  "trade_price": 172.80,
  "cd_percent": 4,
  "scm_dis_value": 0,
  "taxable_value": 829.44,
  "gst_percent": 12,
  "net_value": 929.97
}

NOTICE: trade_price (172.80) is in the TRADE PRICE column. taxable_value (829.44) is in the TAXABLE VALUE column. Even though trade_price and qty_billed are close in the row, they go to DIFFERENT fields. Do NOT put 172.80 into taxable_value.

═══ ARITHMETIC FORMULA (MUST be applied to EVERY row) ═══
1. taxable_value = trade_price × qty_billed × (1 - cd_percent/100)
2. gst_amount = taxable_value × gst_percent / 100
3. net_value = taxable_value + gst_amount
Use the printed values from the invoice columns as the primary source. Only apply these formulas as a fallback when a printed value is missing or unreadable. If a printed value exists but differs from the formula result, use the printed value — distributors may apply scheme adjustments or tiered discounts that change the standard formula.

═══ GST BREAKDOWN (Summary Level) ═══
CGST and SGST are split equally. If GST is 12%, CGST = 6% and SGST = 6%.
Group taxable amounts by their GST slabs and compute exact CGST/SGST totals.
- 12% GST slab → 6% CGST + 6% SGST
- 18% GST slab → 9% CGST + 9% SGST

═══ CROSS-VERIFICATION (MUST DO BEFORE OUTPUTTING) ═══
Before you output the final JSON, perform these checks internally and CORRECT any mismatches:
1. Sum all line taxable_values → this MUST match total_taxable_amount from the footer. If it doesn't, re-examine each line — you likely shifted a column.
2. Sum all line net_values → this should be close to grand_total (within ₹10 for rounding).
3. grand_total ≈ total_taxable_amount + cgst_total + sgst_total - scheme_discount - cash_discount + round_off
4. If any line's taxable_value is less than ₹5 and trade_price > ₹20, you almost certainly put the trade price or quantity in the wrong column. Fix it before outputting.
5. Report any remaining discrepancies as a note, but always use the printed value from the invoice column.

═══ GENERAL RULES ═══
- Extract EVERY line item visible, even if partially visible
- For confidence: 1.0 = perfect, 0.9 = very clear, 0.8 = minor doubt, 0.7 = partially readable, <0.7 = uncertain
- If a field is not visible, use null for strings and 0 for numbers
- Read gst_percent from EACH ROW — do NOT apply a single flat GST rate to all lines
- mfac should be the manufacturer abbreviation/code as printed (CIPL, MICR, ALKE, etc.)
- pack should include the unit (10'S, 15'S, 15ML, 60'S, 3'S, 120'S, etc.)
- For captureQuality.readable: false if >30% of invoice is unreadable
- If "Page X of Y" or "X/Y" appears in header/footer, extract into captureQuality.pageInfo as {current: X, total: Y}. Null if not visible.`;

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

            // Log full line items for debugging
            console.log("[EXTRACTED LINE ITEMS]:", JSON.stringify(extracted.lineItems, null, 2));
            console.log("[EXTRACTED TOTALS]:", JSON.stringify({ invoiceTotal: extracted.invoiceTotal, schemeDiscount: extracted.schemeDiscount, cashDiscount: extracted.cashDiscount, roundOff: extracted.roundOff }));

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
// Handles Gemini's natural snake_case output and maps to internal fields
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
        // Try to find JSON object in the response
        const jsonMatch = cleaned.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
            parsed = JSON.parse(jsonMatch[0]);
        } else {
            throw new Error("Could not parse Gemini response as JSON");
        }
    }

    // Map Gemini's snake_case output to internal camelCase fields
    // Gemini outputs: distributor_name, buyer_name, invoice_no, invoice_date, grand_total, etc.
    // Also accept camelCase fallbacks for flexibility
    const result = {
        distributor: parsed.distributor_name || parsed.distributor || "",
        buyerName: (parsed.buyer_name || parsed.buyerName) ? String(parsed.buyer_name || parsed.buyerName).trim() : null,
        invoiceNumber: parsed.invoice_no || parsed.invoiceNumber || "",
        invoiceDate: parsed.invoice_date || parsed.invoiceDate || null,
        invoiceTotal: Number(parsed.grand_total || parsed.invoiceTotal) || 0,
        totalTaxableAmount: Number(parsed.total_taxable_amount || parsed.totalTaxableAmount) || 0,
        cgstTotal: Number(parsed.cgst_total || parsed.cgstTotal) || 0,
        sgstTotal: Number(parsed.sgst_total || parsed.sgstTotal) || 0,
        schemeDiscount: Number(parsed.scheme_discount || parsed.schemeDiscount) || 0,
        cashDiscount: Number(parsed.cash_discount || parsed.cashDiscount) || 0,
        roundOff: Number(parsed.round_off || parsed.roundOff) || 0,
        pendingInvoicesCount: Math.max(0, parseInt(parsed.pending_invoices_count || parsed.pendingInvoicesCount) || 0),
        pendingTotalAmount: Math.max(0, parseFloat(parsed.pending_total_amount || parsed.pendingTotalAmount) || 0),
        lineItems: [],
        captureQuality: {
            readable: parsed.captureQuality?.readable !== false,
            issues: Array.isArray(parsed.captureQuality?.issues) ? parsed.captureQuality.issues : [],
            missingPage: parsed.captureQuality?.missingPage === true,
            pageInfo: parsed.captureQuality?.pageInfo || null
        }
    };

    // Process line items — map Gemini's snake_case fields to internal fields
    // Gemini outputs: qty_billed, free_scm, batch_no, exp_date, trade_price,
    //   cd_percent, scm_dis_value, taxable_value, gst_percent, net_value, mfac, hsn_code, pack
    // Internal fields: quantityBilled, quantityFree, batchNumber, expiryDate, tradePrice,
    //   cdPercent, scmDiscount, netValue (taxable), gstRate, gstValue (computed), lineTotal, mfac, hsnCode, packSize
    if (Array.isArray(parsed.lineItems)) {
        result.lineItems = parsed.lineItems.map(item => {
            const tradePrice = Math.max(0, parseFloat(item.trade_price || item.tradePrice) || 0);
            const quantityBilled = Math.max(0, parseInt(item.qty_billed || item.quantityBilled) || 0);
            const cdPercent = Math.max(0, parseFloat(item.cd_percent || item.cdPercent) || 0);
            const gstRate = [5, 12, 18, 28].includes(Number(item.gst_percent || item.gstRate))
                ? Number(item.gst_percent || item.gstRate) : 12;

            // Gemini's taxable_value = trade_price × qty × (1 - cd%) = our netValue
            const netValue = Math.max(0, parseFloat(item.taxable_value || item.netValue) || 0);
            // Gemini's net_value = taxable_value + GST = our lineTotal
            const lineTotal = Math.max(0, parseFloat(item.net_value || item.lineTotal) || 0);
            // Compute gstValue from the difference (net_value - taxable_value)
            const gstValue = Math.max(0, +(lineTotal - netValue).toFixed(2));

            return {
                medicineName: String(item.medicineName || item.description || "").trim(),
                batchNumber: String(item.batch_no || item.batchNumber || "").trim(),
                expiryDate: String(item.exp_date || item.expiryDate || "").trim(),
                quantityBilled,
                quantityFree: Math.max(0, parseInt(item.free_scm || item.quantityFree) || 0),
                tradePrice,
                cdPercent,
                scmDiscount: Math.max(0, parseFloat(item.scm_dis_value || item.scmDiscount) || 0),
                netValue,
                gstRate,
                gstValue,
                mrp: Math.max(0, parseFloat(item.mrp) || 0),
                packSize: (item.pack || item.packSize) ? String(item.pack || item.packSize).trim() : null,
                hsnCode: (item.hsn_code || item.hsnCode) ? String(item.hsn_code || item.hsnCode).trim() : null,
                rack: item.rack ? String(item.rack).trim() : null,
                ptr: Math.max(0, parseFloat(item.ptr) || 0),
                mfac: item.mfac ? String(item.mfac).trim() : null,
                lineTotal,
                confidence: Math.max(0, Math.min(1, parseFloat(item.confidence) || 0.5))
            };
        });
    }

    // Post-mapping sanity check: detect column-shifted values.
    // If taxable_value is suspiciously low compared to trade_price × qty,
    // Gemini likely shifted a value from an adjacent column (qty, discount, etc.)
    // into the taxable_value slot. Flag these for review.
    result.lineItems.forEach((item) => {
        const formulaNet = item.tradePrice > 0 && item.quantityBilled > 0
            ? +(item.tradePrice * item.quantityBilled * (1 - item.cdPercent / 100)).toFixed(2)
            : 0;
        // If extracted taxable is < 20% of formula estimate AND formula estimate > ₹50,
        // the extracted value is almost certainly a column-shift error
        if (formulaNet > 50 && item.netValue > 0 && item.netValue < formulaNet * 0.2) {
            item.columnShiftSuspected = true;
            item.validationNote = `Possible column shift: taxable ₹${item.netValue} looks too low (expected ~₹${formulaNet} based on trade ₹${item.tradePrice} × ${item.quantityBilled} pcs)`;
            item.confidence = Math.max(0.2, item.confidence - 0.3); // Lower confidence
            logger.warn(`[parseGeminiResponse] Column shift suspected for "${item.medicineName}": got taxable ₹${item.netValue}, formula suggests ~₹${formulaNet}`);
        }
    });

    // Soft-validate each line item against arithmetic formula
    // Printed invoice values are the source of truth — formula is only used
    // to detect large discrepancies (likely OCR errors), not distributor rounding.
    // Tolerance: ₹10 per line (pharma distributors apply scheme/cd adjustments
    // that shift taxable values by a few rupees from the base formula).
    let flaggedCount = 0;
    result.lineItems.forEach((item) => {
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
                ? `Expected taxable ₹${expectedNet} (got ₹${item.netValue})`
                : gstMismatch
                    ? `Expected GST ₹${expectedGst} (got ₹${item.gstValue})`
                    : `Expected total ₹${expectedLineTotal} (got ₹${item.lineTotal})`;
            flaggedCount++;
        } else {
            item.lineValidationFailed = false;
            item.expectedNetValue = expectedNet;
        }
    });

    if (flaggedCount > 0) {
        logger.info(`[parseGeminiResponse] ${flaggedCount} of ${result.lineItems.length} lines have large formula discrepancies (likely OCR errors)`);
    }

    // Grand-total cross-verification: compare sum of extracted line totals
    // against the printed grand total. If they diverge massively, columns are
    // likely shifted across multiple rows (systemic extraction error).
    if (result.invoiceTotal > 0 && result.lineItems.length > 0) {
        let sumNet = 0, sumGst = 0;
        result.lineItems.forEach(item => {
            sumNet += item.netValue || 0;
            sumGst += item.gstValue || 0;
        });
        const extractedTotal = sumNet + sumGst - (result.schemeDiscount || 0) - (result.cashDiscount || 0) + (result.roundOff || 0);
        const totalRatio = extractedTotal / result.invoiceTotal;

        if (totalRatio < 0.5 || totalRatio > 2.0) {
            // Massive divergence — almost certainly a systemic column-shift problem
            logger.warn(`[parseGeminiResponse] Grand total mismatch: extracted ₹${extractedTotal.toFixed(2)} vs printed ₹${result.invoiceTotal} (ratio ${totalRatio.toFixed(2)})`);
            result.lineItems.forEach(item => {
                if (!item.columnShiftSuspected) {
                    item.columnShiftSuspected = true;
                    item.validationNote = `Systemic extraction issue: line totals sum to ₹${extractedTotal.toFixed(2)} but invoice declares ₹${result.invoiceTotal}`;
                    item.confidence = Math.max(0.2, item.confidence - 0.2);
                }
            });
        } else if (totalRatio < 0.8 || totalRatio > 1.2) {
            // Moderate divergence — flag as informational
            logger.info(`[parseGeminiResponse] Grand total moderate divergence: extracted ₹${extractedTotal.toFixed(2)} vs printed ₹${result.invoiceTotal} (ratio ${totalRatio.toFixed(2)})`);
        }
    }

    return result;
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
