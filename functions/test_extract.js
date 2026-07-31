/**
 * Local test harness for extractInvoice.
 * Runs the exact production prompt + schema against LOCAL image files
 * (no Storage, no Firebase). Usage:
 *   set GEMINI_API_KEY=<key>
 *   node test_extract.js <image1> [image2 ...]
 */
const fs = require("fs");
const { GoogleGenerativeAI } = require("@google/generative-ai");

const PROMPT = `You are an advanced, high-precision OCR and invoice parsing engine specialized in Indian Pharmaceutical GST Invoices. Extract line items, metadata, invoice-level adjustments, and financial totals with 100% accounting alignment.

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
   If the footer is genuinely unreadable or off-page, set captureQuality.readable = false (or missingPage = true) and explain in issues[].
`;

const apiKey = process.env.GEMINI_API_KEY;
if (!apiKey) {
  console.error("Set GEMINI_API_KEY env var first.");
  process.exit(1);
}

const files = process.argv.slice(2);
if (files.length === 0) {
  console.error("Usage: node test_extract.js <image1> [image2 ...]");
  process.exit(1);
}

const imageParts = files.map((f) => {
  const buf = fs.readFileSync(f);
  const ext = f.split(".").pop().toLowerCase();
  const mime = ext === "png" ? "image/png" : ext === "pdf" ? "application/pdf" : "image/jpeg";
  return {
    inlineData: { data: buf.toString("base64"), mimeType: mime },
  };
});

const genAI = new GoogleGenerativeAI(apiKey);
const model = genAI.getGenerativeModel({ model: "gemini-3.5-flash-lite" });

(async () => {
  try {
    const result = await model.generateContent([PROMPT, ...imageParts]);
    const text = result.response.text();
    const cleaned = text
      .replace(/^```json\s*/i, "")
      .replace(/^```\s*/i, "")
      .replace(/```\s*$/i, "")
      .trim();
    const parsed = JSON.parse(cleaned);
    console.log(JSON.stringify(parsed, null, 2));
  } catch (err) {
    console.error("ERROR:", err.message);
    process.exit(1);
  }
})();
