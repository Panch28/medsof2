/**
 * Local test harness for extractInvoice.
 * Runs the exact production prompt + schema against LOCAL image files
 * (no Storage, no Firebase). Usage:
 *   set GEMINI_API_KEY=<key>
 *   node test_extract.js <image1> [image2 ...]
 */
const fs = require("fs");
const { GoogleGenerativeAI } = require("@google/generative-ai");

const PROMPT = `You are an expert AI OCR & Accounting Parser for Indian Pharmaceutical GST Invoices (distributor formats like Vardhman Medisales / MCS software). Extract line items, metadata, invoice-level adjustments, and financial totals with 100% accounting alignment.

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
    b. Verify the footer formula: saleValue − schDisc − cashDiscount + totalGst + roundOff − cnNo should approximately equal grandTotal (within ₹1). If it does not, you MISREAD a footer field — re-scan the footer summary block and CORRECT the specific misread field from the printed digits. Do NOT change line items to fake a match.
    c. PER-LINE GST RATE check: for EVERY line, gstValue should be within ₹1 of (taxableValue × gstRate / 100). If a line's gstRate disagrees with its gstValue, you MISREAD the GST % column — re-scan that line and correct gstRate from the printed digits.
    d. RATE-SLAB check: the DISTINCT gstRate values across lines must match the GST % slabs shown in the footer tax table. A footer row "CGST 9% + SGST 9%" means those lines are taxed at 18%; "CGST 2.5% + SGST 2.5%" means 5%. If your lines show ONLY ONE rate but the footer shows TWO or MORE slabs, you FLATTENED the GST % column — re-scan EVERY line and read each actual per-line rate.
    e. PER-LINE NET check: for EVERY line, netValue should be within ₹1 of (taxableValue + gstValue). If a line's netValue disagrees with taxableValue + gstValue, you likely SWAPPED the Taxable and Net columns — re-scan that line and read the two printed amounts back into their correct columns.
    If the footer is genuinely unreadable or off-page, set captureQuality.readable = false (or missingPage = true) and explain in issues[].`;

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

function stripMarkdown(text) {
  return text
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/```\s*$/i, "")
    .trim();
}

function round2(x) {
  return Math.round(x * 100) / 100;
}

function repairColumnSwaps(parsed) {
  const lines = Array.isArray(parsed.lineItems) ? parsed.lineItems : [];
  let repaired = 0;
  for (const l of lines) {
    const t = Number(l.taxableValue) || 0;
    const g = Number(l.gstValue) || 0;
    const n = Number(l.netValue) || 0;
    const r = Number(l.gstRate) || 0;
    if (!(t > 0 && g > 0 && n > 0 && r > 0)) continue;
    if (Math.abs(t + g - n) <= 1) continue;
    if (Math.abs(n * (1 + r / 100) - t) > 0.5) continue;
    if (Math.abs((t - n) - (n * r) / 100) > 0.5) continue;
    l.taxableValue = round2(n);
    l.netValue = round2(t);
    l.gstValue = round2(t - n);
    repaired++;
  }
  return repaired;
}

function checkGstConsistency(parsed) {
  const issues = [];
  const lines = Array.isArray(parsed.lineItems) ? parsed.lineItems : [];
  const summary = parsed.invoiceSummary || {};

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
          issues.push(`footer GST refs disagree: ${refs[i].src} Rs.${refs[i].v.toFixed(2)} vs ${refs[j].src} Rs.${refs[j].v.toFixed(2)}`);
        }
      }
    }
  }

  const lineGstSum = lines.reduce((s, l) => s + (Number(l.gstValue) || 0), 0);
  for (const ref of refs) {
    if (Math.abs(lineGstSum - ref.v) > 1) {
      issues.push(`line GST sum Rs.${lineGstSum.toFixed(2)} != printed ${ref.src} Rs.${ref.v.toFixed(2)}`);
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
        issues.push(`"${l.medicineName || "?"}": gstValue Rs.${gv.toFixed(2)} != taxable Rs.${taxable.toFixed(2)} x ${rate}% = Rs.${expected.toFixed(2)} (gstRate or gstValue misread)`);
      }
    }
    if (taxable > 0 && gv > 0 && net > 0) {
      const expectedNet = taxable + gv;
      if (Math.abs(net - expectedNet) > 1) {
        issues.push(`"${l.medicineName || "?"}": netValue Rs.${net.toFixed(2)} != taxable Rs.${taxable.toFixed(2)} + GST Rs.${gv.toFixed(2)} = Rs.${expectedNet.toFixed(2)} (columns misread)`);
      }
    }
  }

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
          issues.push(`footer formula implies GST Rs.${implied.toFixed(2)} but ${ref.src} is Rs.${ref.v.toFixed(2)}`);
        }
      }
    }
  }

  return issues;
}

(async () => {
  try {
    let text;
    let parsed;
    let result = await model.generateContent([PROMPT, ...imageParts]);
    text = result.response.text();
    parsed = JSON.parse(stripMarkdown(text));

    let gstIssues = [];
    let gstAttempts = 0;
    for (let attempt = 0; attempt <= 3; attempt++) {
      repairColumnSwaps(parsed);
      gstIssues = checkGstConsistency(parsed);
      gstAttempts = attempt + 1;
      if (gstIssues.length === 0) break;
      if (attempt === 3) break; // correction budget exhausted; final state already checked

      console.warn(`[GST check failed - attempt ${attempt + 1}]`);
      gstIssues.forEach((i) => console.warn("  - " + i));

      const lineGstSum = (parsed.lineItems || []).reduce((s, l) => s + (Number(l.gstValue) || 0), 0);
      const refsInfo = [];
      const totalGst = Number(parsed.invoiceSummary?.totalGst) || 0;
      const cst =
        (Number(parsed.invoiceSummary?.totalCGST) || 0) +
        (Number(parsed.invoiceSummary?.totalSGST) || 0) +
        (Number(parsed.invoiceSummary?.totalIGST) || 0);
      if (totalGst > 0) refsInfo.push(`Total GST Rs.${totalGst.toFixed(2)}`);
      if (cst > 0) refsInfo.push(`CGST+SGST+IGST Rs.${cst.toFixed(2)}`);
      const printedRef = refsInfo.length ? refsInfo.join(", ") : "not extracted";
      const correction = `You previously returned this JSON for the invoice images:\n\n${JSON.stringify(parsed)}\n\nThe following deterministic GST checks FAILED against the printed figures:\n- ${gstIssues.join("\n- ")}\n\nThe printed footer GST totals are GROUND TRUTH and equal: ${printedRef}. Your line GST sum is Rs.${lineGstSum.toFixed(2)} — it must equal the printed total within Rs.1.\nRe-read the ACTUAL printed digits of EVERY line: the GST % column, the GST Rs. column, and the Taxable/Net columns. If a line is internally broken (taxableValue + gstValue != netValue) the Taxable and Net columns were probably SWAPPED — swap them back from the printed digits.\nCorrect any of gstRate, gstValue, taxableValue, netValue (only to repair a column swap or rate misread) and the footer totalGst / totalCGST / totalSGST / totalIGST / saleValue / grandTotal / schDisc / roundOff / cnNo if misread, so that:\n1. sum of ALL line gstValue == printed Total GST (within Rs.1)\n2. for EVERY line: gstValue == taxableValue x gstRate / 100 (within Rs.1)\n3. for EVERY line: netValue == taxableValue + gstValue (within Rs.1)\n4. the distinct gstRate values match the GST % slabs in the footer tax table.\nRe-read the ACTUAL printed digits of every line's GST % column — NEVER assume or default a rate such as 12.\nDo NOT change medicineName, batchNumber, expiryDate, quantities, unitPrice, cdPercent, cdValue, or the Cash Discount.\nReturn ONLY the corrected JSON with the SAME schema — no markdown, no explanation.`;
      result = await model.generateContent([correction, ...imageParts]);
      text = result.response.text();
      parsed = JSON.parse(stripMarkdown(text));
    }
    if (gstIssues.length > 0) {
      console.error("[GST still inconsistent after retries]");
      gstIssues.forEach((i) => console.error("  - " + i));
    }

    console.log(JSON.stringify({ gstCheck: { pass: gstIssues.length === 0, attempts: gstAttempts, issues: gstIssues }, ...parsed }, null, 2));
  } catch (err) {
    console.error("ERROR:", err.message);
    process.exit(1);
  }
})();
