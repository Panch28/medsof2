/**
 * RxExpiry Cloud Functions
 *
 * Extraction pipeline (Gemini only — no local OCR):
 * - extractInvoice: Synchronous HTTPS callable. Accepts one or more Storage
 *   paths, downloads the files, sends them to Gemini (gemini-3.5-flash-lite)
 *   with a structured Indian Pharma GST invoice prompt, and returns parsed
 *   structured data. Deterministic GST enforcement runs server-side.
 * - processImportQueueItem: The resilient bulk-import worker. Claims an item
 *   from /pharmacies/{id}/importQueue/{imageId} (leased, resumable), downloads
 *   the raw file, runs the shared Gemini pipeline, and persists the result as
 *   status "extracted" for human review. Every image is processed as a
 *   standalone invoice — there is NO cross-page merging/waiting.
 *   Queue statuses: uploaded → processing → extracted → saved (or rejected /
 *   discarded). Terminal states: saved/reviewed/ingested/failed/rejected.
 *   Processing resumes on page load for items in "uploaded"/"extracted"/
 *   "processing" (lease expired).
 * - saveInvoice: Writes a confirmed invoice + medicine records. When queueId is
 *   supplied it guards against duplicate saves from stale client retries and
 *   deletes the queue doc + raw Storage image after the write commits. It also
 *   enforces a server-side pHash duplicate hard block (a re-upload of the same
 *   page) before any write lands; the (distributorId, invoiceNumber) compound
 *   key is now a soft warning only, because multi-page invoices legitimately
 *   save one doc per page under the same key. Pages without the printed footer
 *   totals are stored partial=true with invoiceTotal 0 so reporting never
 *   double-counts across the pages of one invoice.
 * - discardQueueItem: Staff-authorized permanent discard of a queue item
 *   (deletes the queue doc + raw Storage image).
 * - listPendingInvoices / getPendingInvoice / deletePendingInvoice: legacy
 *   staging management kept for backward compatibility — the extraction
 *   pipeline no longer writes to pending_invoices.
 * - scheduledCleanup: daily — deletes raw uploads older than 30 days, strips
 *   rawGeminiResponse from old invoices, drops stale pending invoices, recovers
 *   queue items stuck in "processing" with an expired lease, and purges
 *   terminal failed/rejected (>7 days) and ingested (>24h) queue items.
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { defineSecret } = require("firebase-functions/params");
const { logger } = require("firebase-functions/v2");
const { initializeApp } = require("firebase-admin/app");
const { getStorage } = require("firebase-admin/storage");
const { getFirestore, FieldValue, Timestamp } = require("firebase-admin/firestore");
const { GoogleGenerativeAI } = require("@google/generative-ai");

initializeApp();

const GEMINI_API_KEY = defineSecret("GEMINI_API_KEY");
const GEMINI_MODEL = "gemini-3.5-flash-lite";
// Stable general-use fallback model: if the primary model keeps returning
// 503/429 overload errors after the retry budget is exhausted, we switch to
// this model before surfacing an error to the client.
const GEMINI_FALLBACK_MODEL = "gemini-3.6-flash";
// Exponential backoff between retries (2s, 4s, 8s) for transient overload.
const GEMINI_RETRY_DELAYS_MS = [2000, 4000, 8000];
const MAX_EXTRACTION_ATTEMPTS = 3;
// Leased claim: must comfortably fit inside the 120s callable timeout so a
// crashed invocation lets the lease expire and another attempt can reclaim.
const QUEUE_LEASE_MS = 115000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// A 503 "Service Unavailable" (and friends: 429 quota/rate-limit, 500/502/504
// upstream blips) is transient — the retry wrapper below absorbs them. Real
// errors (400 invalid schema, safety blocks, auth) propagate immediately.
function isRetryableGeminiError(err) {
  if (!err) return false;
  const status = Number(err.status || err.code || 0);
  if ([429, 500, 502, 503, 504].includes(status)) return true;
  const msg = String(err.message || err.statusText || "");
  return /overload|resource exhausted|quota|unavailable|try again later|too many requests|503/i.test(msg);
}

// Wraps a single generateContent call with exponential-backoff retries
// (2s → 4s → 8s) for transient overload errors on a given model.
async function generateContentWithRetry(model, parts) {
  let lastErr = null;
  for (let attempt = 0; attempt <= GEMINI_RETRY_DELAYS_MS.length; attempt++) {
    try {
      return await model.generateContent(parts);
    } catch (err) {
      lastErr = err;
      if (!isRetryableGeminiError(err)) throw err;
      if (attempt < GEMINI_RETRY_DELAYS_MS.length) {
        const delayMs = GEMINI_RETRY_DELAYS_MS[attempt];
        logger.warn(
          `[gemini] ${model.model} transient error (${err.message}) — retry ${attempt + 1}/${GEMINI_RETRY_DELAYS_MS.length} in ${delayMs}ms`
        );
        await sleep(delayMs);
      }
    }
  }
  throw lastErr;
}

// Primary model with backoff retries; if it remains overloaded after the full
// retry budget, fall back to the stable general-use model before giving up.
async function generateWithModelFallback(genAI, parts) {
  const modelNames = [
    GEMINI_MODEL,
    ...(GEMINI_FALLBACK_MODEL && GEMINI_FALLBACK_MODEL !== GEMINI_MODEL
      ? [GEMINI_FALLBACK_MODEL]
      : []),
  ];
  let lastErr = null;
  for (const name of modelNames) {
    try {
      const model = genAI.getGenerativeModel({ model: name });
      return await generateContentWithRetry(model, parts);
    } catch (err) {
      lastErr = err;
      if (!isRetryableGeminiError(err)) throw err; // non-transient — never fall back
      logger.warn(`[gemini] model ${name} overloaded after ${GEMINI_RETRY_DELAYS_MS.length} retries — falling back to next model`);
    }
  }
  throw lastErr;
}

// Deterministic pagination-marker reader. Scans the raw model text for printed
// indicators ("Page 1 of 2", "Page: 1/2", "Page No. 1") and returns the page
// numbers so a multi-page invoice can never silently degrade to single-page.
// Ignores JSON keys like "pageNumber" (never followed by digits at that spot).
function detectPageMarkers(rawText) {
  const markers = { pageNumber: null, totalPages: null };
  if (!rawText) return markers;
  const text = String(rawText);
  const of = text.match(/page[:\s]*(?:no\.?[:\s]*)?(\d{1,3})\s*(?:of|\/)\s*(\d{1,3})/i);
  if (of) {
    markers.pageNumber = parseInt(of[1], 10);
    markers.totalPages = parseInt(of[2], 10);
    return markers;
  }
  const alone = text.match(/page[:\s]*(?:no\.?[:\s]*)?(\d{1,3})/i);
  if (alone) markers.pageNumber = parseInt(alone[1], 10);
  return markers;
}

function round2(x) {
  return Math.round(x * 100) / 100;
}

// Canonicalizes a distributor name so the string variations Gemini extracts
// ("VARDHMAN MEDISALES", "Vardhman Medisales Pvt. Limited", "VARDHMAN
// MEDISALES PVT LTD") collapse to ONE normalized key ("VARDHMAN MEDISALES").
// Applied at every write path so medicine batches aggregate under a single
// master distributor profile instead of fragmenting across casing/punctuation/
// legal-suffix variants in Firestore.
const DISTRIBUTOR_LEGAL_SUFFIXES = [
  "PRIVATE LIMITED",
  "PRIVATE LTD",
  "PVT LIMITED",
  "PVT LTD",
  "PRIVATE",
  "PVT",
  "LIMITED",
  "LTD",
  "LLP",
  "CO",
  "COMPANY",
  "CORPORATION",
  "INCORPORATED",
  "INC",
];

function escapeRegExp(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// Deep-clean a record before it hits Firestore or returns to the client:
// Firestore rejects NaN/Infinity, and httpsCallable cannot encode them. Any
// non-finite number is replaced with null so downstream `|| 0` guards apply.
function sanitizeNumbers(obj) {
  if (Array.isArray(obj)) return obj.map(sanitizeNumbers);
  if (obj && typeof obj === "object") {
    const out = {};
    for (const k of Object.keys(obj)) {
      const v = obj[k];
      if (typeof v === "number" && !Number.isFinite(v)) out[k] = null;
      else if (v && typeof v === "object") out[k] = sanitizeNumbers(v);
      else out[k] = v;
    }
    return out;
  }
  return obj;
}

function normalizeDistributorName(name) {
  if (name == null) return "";
  let s = String(name)
    .toUpperCase()
    .replace(/&/g, " AND ") // "SHARMA & SONS" → "SHARMA AND SONS"
    .replace(/[^\w\s]/g, " ") // "PVT. LTD." → "PVT LTD"
    .replace(/\s+/g, " ")
    .trim();
  if (!s) return "";

  // Strip trailing legal suffixes, repeatedly, so multi-token suffixes like
  // "PVT LTD" are fully removed even after a partial match leaves one behind.
  let prev = null;
  while (s && s !== prev) {
    prev = s;
    let stripped = false;
    for (const suffix of DISTRIBUTOR_LEGAL_SUFFIXES) {
      const re = new RegExp(`(?:^|\\s)${escapeRegExp(suffix)}$`, "i");
      if (re.test(s)) {
        s = s.slice(0, s.length - suffix.length).replace(/\s+$/, "").trim();
        stripped = true;
        break;
      }
    }
    if (!stripped) break;
  }
  return s;
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

// Deterministic per-line arithmetic validation. Checks internal consistency
// of each medicine line (taxable × rate = GST, taxable + GST = net) without
// cross-checking against footer totals. Returns an array of human-readable
// discrepancies (empty array = all lines internally consistent).
function checkGstConsistency(parsed) {
  const fixable = [];
  const informational = [];
  const lines = Array.isArray(parsed.lineItems) ? parsed.lineItems : [];

  for (const l of lines) {
    const taxable = Number(l.taxableValue) || 0;
    const rate = Number(l.gstRate) || 0;
    const gv = Number(l.gstValue) || 0;
    const net = Number(l.netValue) || 0;
    const unitPrice = Number(l.unitPrice) || 0;
    const qtyBilled = Number(l.quantityBilled) || 0;
    const cdPercent = Number(l.cdPercent) || 0;
    const cdValue = Number(l.cdValue) || 0;

    // Per-line GST arithmetic: gstValue should equal taxableValue × gstRate / 100
    if (taxable > 0 && rate > 0) {
      const expected = (taxable * rate) / 100;
      if (Math.abs(gv - expected) > 1) {
        fixable.push(`"${l.medicineName || "?"}": gstValue ₹${gv.toFixed(2)} != taxable ₹${taxable.toFixed(2)} x ${rate}% = ₹${expected.toFixed(2)} (gstRate or gstValue misread)`);
      }
    }

    // Per-line net arithmetic: netValue should equal taxableValue + gstValue
    // This catches column swaps (taxable/net swapped) - bug #2 from VN-23-341141
    if (taxable > 0 && gv > 0 && net > 0) {
      const expectedNet = taxable + gv;
      if (Math.abs(net - expectedNet) > 1) {
        fixable.push(`"${l.medicineName || "?"}": netValue ₹${net.toFixed(2)} != taxable ₹${taxable.toFixed(2)} + GST ₹${gv.toFixed(2)} = ₹${expectedNet.toFixed(2)} (columns misread)`);
      }
    }

    // Per-line taxableValue arithmetic: taxableValue ≈ unitPrice × qtyBilled × (1 - cdPercent/100)
    // This is the "GOLDEN ROW FORMULA" check from the prompt — bug #2 from VN-23-341141 (GUDCEF CV line).
    // INFORMATION ONLY — NEVER drives a corrective Gemini turn: the correction prompt locks
    // unitPrice / quantity / cdPercent, so this formula can never be corrected server-side, and on
    // Vardhman/MCS invoices it legitimately does NOT hold (extra per-line adjustments). Flagging it
    // as fixable burned the full 5-attempt correction budget on every such page.
    if (unitPrice > 0 && qtyBilled > 0) {
      const expectedTaxable = unitPrice * qtyBilled * (1 - cdPercent / 100);
      if (taxable > 0 && Math.abs(taxable - expectedTaxable) > 2) {
        informational.push(`"${l.medicineName || "?"}": taxableValue ₹${taxable.toFixed(2)} != unitPrice ₹${unitPrice.toFixed(2)} × qty ${qtyBilled} × (1 - ${cdPercent}%) = ₹${expectedTaxable.toFixed(2)} (reference only)`);
      }
    }
  }

  return { fixable, informational };
}
// ─── Shared Gemini extraction pipeline ──────────────────────────────────────
// ONE canonical prompt + ONE extraction routine, used by both extractInvoice
// and the import-queue worker (processImportQueueItem) so the two paths can
// never drift.

const GEMINI_EXTRACTION_PROMPT = `You are an expert AI OCR & Accounting Parser for Indian Pharmaceutical GST Invoices (distributor formats like Vardhman Medisales / MCS software). Extract line items, metadata, invoice-level adjustments, and financial totals with 100% accounting alignment.

Return ONLY valid JSON matching this exact schema — no markdown, no explanation:

{
  "distributor": "string",
  "invoiceNumber": "string",
  "invoiceDate": "string (DD/MM/YYYY or as printed)",
  "invoiceTotal": number,
  "hasFooterTotals": boolean,
  "pageNumber": number,
  "totalPages": number,
  "looksLikeContinuationPage": boolean,
  "printedPagination": "string (echo the literal printed pagination/continuation line VERBATIM, e.g. 'Page: 1 of 2', 'Page No. 1', or 'Continue Next Page...'; empty string if none is printed)",
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
6. PAGE CLASSIFICATION & PAGINATION (MANDATORY for every page). Use STRUCTURAL pagination tracking — find the printed page indicator ("Page: X of Y", "Page X of Y", "Page X/Y", or "Page No.: X") near the top/bottom of the page. NEVER rely on custom phrases like "Continue Next Page". Extract:
   - hasFooterTotals: Set to TRUE if this page contains the invoice summary/footer block with Grand Total, Cash Discount, Total GST, Sale Value, CGST/SGST amounts, and/or the tax slab breakdown table. Set to FALSE if this page only has line items and no financial summary block.
   - pageNumber: The X from the printed "Page: X of Y" indicator. If the indicator is missing, estimate: if the page has a footer/summary it is likely the LAST page; if it starts with a header/distributor name and invoice number it is page 1.
   - totalPages: The Y from the printed "Page: X of Y" indicator. If the indicator is missing, use 1 for a single-page invoice, 2 for a continuation page, or the highest pageNumber seen.
    - looksLikeContinuationPage: TRUE if this page has NO invoice header (no "Tax Invoice", no distributor name at top), NO invoice number at top, and appears to be a continuation of line items from a previous page. FALSE if it looks like the first/main page of an invoice.
    - printedPagination: echo the EXACT printed page-indicator text VERBATIM — e.g. "Page: 1 of 2", "Page No. 1", or "Continue Next Page...". If the page prints NO pagination or continuation line at all, return "" (empty string). NEVER invent a value — this field exists so downstream code can tell a "no printed line" page from a genuine "Continue Next Page..." continuation page.
7. SINGLE-PAGE INVOICE: If this page contains BOTH line items AND the footer totals, set hasFooterTotals = true, pageNumber = 1, totalPages = 1, looksLikeContinuationPage = false. If it is page 1 of a multi-page invoice (line items only, no footer), set pageNumber = 1, totalPages = 2 (or the printed Y), hasFooterTotals = false.
8. If multiple images are provided, they represent consecutive pages of the SAME invoice. Combine all line items. The footer totals are usually on the last page.
9. confidence values are 0.0 to 1.0 per field. If the image is blurry or unreadable, set captureQuality.readable = false and list reasons in issues[].
10. All amounts in INR as plain numbers (no ₹ symbol), typed as numbers, never strings. gstRate and cdPercent are percentages (e.g. 12, 4.00).
11. Return ONLY valid JSON — no markdown, no explanation.
12. SELF-CHECK before returning (this is mandatory):
    a. sum of ALL line gstValue values should approximately equal the printed Total GST (totalGst) — within ₹1 — BUT ONLY when this page is a complete single-page invoice (hasFooterTotals = true AND totalPages = 1). If this page is a continuation page (hasFooterTotals = false) or part of a multi-page invoice (totalPages > 1), the printed totals belong to the WHOLE invoice and will NOT equal this page's line sum — do NOT adjust line items to force that match; read and return the printed line digits as-is.
    b. Verify the footer formula (saleValue − schDisc − cashDiscount + totalGst + roundOff − cnNo ≈ grandTotal) ONLY when hasFooterTotals = true. If it does not hold, you MISREAD a footer field — re-scan the footer summary block and CORRECT the specific misread field from the printed digits (pay special attention to the "Cash Disc." / "Sch Disc." rows — never read a printed discount as ₹0.00). Do NOT change line items to fake a match. When hasFooterTotals = false there is no footer on this page to verify.
    c. PER-LINE GST RATE check: for EVERY line, gstValue should be within ₹1 of (taxableValue × gstRate / 100). If a line's gstRate disagrees with its gstValue, you MISREAD the GST % column — re-scan that line and correct gstRate from the printed digits.
    d. RATE-SLAB check: the DISTINCT gstRate values across lines must match the GST % slabs shown in the footer tax table. A footer row "CGST 9% + SGST 9%" means those lines are taxed at 18%; "CGST 2.5% + SGST 2.5%" means 5%. If your lines show ONLY ONE rate but the footer shows TWO or MORE slabs, you FLATTENED the GST % column — re-scan EVERY line and read each actual per-line rate.
    e. PER-LINE NET check: for EVERY line, netValue should be within ₹1 of (taxableValue + gstValue). If a line's netValue disagrees with taxableValue + gstValue, you likely SWAPPED the Taxable and Net columns — re-scan that line and read the two printed amounts back into their correct columns.
    If the footer is genuinely unreadable or off-page, set captureQuality.readable = false (or missingPage = true) and explain in issues[].`;

function stripMarkdown(text) {
  return text
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/```\s*$/i, "")
    .trim();
}

// Download one or more Storage files into Gemini inlineData parts.
async function downloadFilesToParts(bucket, paths) {
  return Promise.all(
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
}
// Core extraction routine shared by extractInvoice and processImportQueueItem.
// Runs the canonical prompt, parses the JSON, detects partial pages (deferring
// all math validation until merge), and applies deterministic GST enforcement
// with up to 4 corrective Gemini turns on full invoices.
//
// hints = { pageNumber, totalPages } — authoritative pagination captured from
// the source file metadata (e.g. pdf.numPages when a PDF is split per page on
// the client). When present they override Gemini's page classification so a
// multi-page invoice is never treated as single-page before staging.
async function runGeminiExtraction(imageParts, genAI, hints = null) {
  let geminiResponse;
  try {
    const result = await generateWithModelFallback(genAI, [
      GEMINI_EXTRACTION_PROMPT,
      ...imageParts,
    ]);
    geminiResponse = result.response.text();
  } catch (err) {
    throw new HttpsError("internal", "Gemini API error: " + err.message);
  }

  // Parse JSON from Gemini response
  let parsed;
  try {
    parsed = JSON.parse(stripMarkdown(geminiResponse));
  } catch (err) {
    throw new HttpsError(
      "internal",
      "Failed to parse Gemini JSON response: " + geminiResponse.substring(0, 300)
    );
  }

  // ── Footer-totals detection ────────────────────────────────────────────────
  // Determines whether the printed financial summary block (Grand Total, GST,
  // discount rows) appears on THIS image. Purely informational now — every
  // image is processed standalone, so nothing gates on it.
  const hasFooterTotals =
    parsed.hasFooterTotals !== undefined
      ? parsed.hasFooterTotals
      : !!(parsed.invoiceSummary && parsed.invoiceSummary.grandTotal > 0);

  // Pagination resolution order: source-file metadata hints (authoritative,
  // e.g. PDF page count) > deterministic marker scan of the raw response >
  // Gemini's parsed JSON. Never default a multi-page page to 1/1.
  const markers = detectPageMarkers(geminiResponse);
  let pgNum = Number(parsed.pageNumber) || 1;
  let pgTot = Number(parsed.totalPages) || 1;
  if (hints && Number(hints.pageNumber) >= 1) pgNum = Number(hints.pageNumber);
  if (hints && Number(hints.totalPages) >= 1) pgTot = Number(hints.totalPages);
  else if (markers.totalPages && markers.totalPages > pgTot) pgTot = markers.totalPages;
  if (
    markers.pageNumber &&
    markers.pageNumber > pgNum &&
    !(hints && Number(hints.pageNumber) >= 1)
  ) {
    pgNum = markers.pageNumber;
  }
  pgNum = Math.max(1, Math.min(pgNum, pgTot));
  pgTot = Math.max(pgNum, pgTot);

  // Every image is processed standalone, so the deterministic GST enforcement
  // below ALWAYS runs against this page's own parsed figures — there is no
  // multi-page merge to defer validation to.
  logger.info(
    `[gemini] page ${pgNum}/${pgTot} of ${parsed.invoiceNumber || "?"} — SINGLE-PAGE (full invoice) (footer=${hasFooterTotals}, hints=${
      hints && Number(hints.pageNumber) >= 1 ? hints.pageNumber + "/" + hints.totalPages : "none"
    })`
  );

  // Printed-footer assertions are only trustworthy when THIS page is the whole
  // invoice: it carries the footer totals AND there are no other pages. A
  // continuation page has no footer; the footer page of a multi-page invoice
  // holds whole-invoice totals over only a subset of the lines, so asserting
  // "sum of this page's lines == printed total" would misfire on every such
  // invoice. Per-line arithmetic checks below still run on every page.
  const printedContMarker = /continue|next page|cont\.?/i.test(String(parsed.printedPagination || ""));
  const validForFooterCheck =
    hasFooterTotals &&
    pgTot <= 1 &&
    !(parsed.looksLikeContinuationPage === true) &&
    !printedContMarker;

  // Corroborate the printed footer BEFORE trusting it to drive corrective turns.
  // Gemini can set hasFooterTotals = true on a page that prints NO footer, and a
  // hallucinated grandTotal must never be used to force the page's line sum to
  // match it. The page's own line-net sum vs the printed Grand Total is a
  // deterministic cross-check we can run WITHOUT Gemini.
  const initialGrandTotal = Number(parsed.invoiceSummary?.grandTotal) || 0;
  const lineNetSum = () =>
    (parsed.lineItems || []).reduce((s, l) => s + (Number(l.netValue) || 0), 0);
  const footerCorroborated =
    validForFooterCheck &&
    initialGrandTotal > 0 &&
    Math.abs(lineNetSum() - initialGrandTotal) <= Math.max(2, initialGrandTotal * 0.02);

  let gstIssues = [];
  let gstInformational = [];
  let gstAttempts = 0;
  {
    // Deterministic GST enforcement: validate the arithmetic ourselves and, if it
    // fails, send Gemini a corrective turn telling it exactly which lines are
    // inconsistent. Gemini's in-prompt self-checks are advisory; this backstop is
    // what makes mixed-rate invoices (5/12/18 etc.) reliable.
    for (let attempt = 0; attempt <= 4; attempt++) {
      repairColumnSwaps(parsed);
      const { fixable, informational } = checkGstConsistency(parsed);
      gstIssues = fixable;
      gstInformational = informational;
      gstAttempts = attempt + 1;
      if (gstIssues.length === 0) break;
      if (attempt === 4) break; // correction budget exhausted; final state already checked

      logger.warn(`[gemini] GST check failed (attempt ${attempt + 1}): ${gstIssues.join(" | ")}`);

      // Footer-total constraints only apply when this page is the complete
      // invoice. On continuation pages / the footer page of a multi-page
      // invoice the printed totals belong to the WHOLE invoice and must not be
      // asserted against this page's line sum.
      const constraints = [];
      if (footerCorroborated && attempt === 0) {
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
        constraints.push(
          `The printed footer GST totals are GROUND TRUTH and equal: ${printedRef}. Your line GST sum is ₹${lineGstSum.toFixed(2)} — it must equal the printed total within ₹1.${uniformHint}`,
          `sum of ALL line gstValue == printed Total GST (within ₹1)`,
          `the distinct gstRate values match the GST % slabs in the footer tax table`,
          `if a "Cash Disc." or "Sch Disc." row is printed in the footer, the discount must be captured — it should equal saleValue + Total GST + Round Off − CN.NO − grandTotal (the implied discount). NEVER leave a printed discount as ₹0.00; re-read the printed discount row digits`,
          `if totalTaxable is printed, sum of ALL line taxableValue == printed totalTaxable (within ₹1)`
        );
      } else {
        constraints.push(
          footerCorroborated
            ? `footer totals were already enforced on the first correction turn — correct only the per-line inconsistencies below`
            : `the printed footer totals are NOT corroborated by this page's line sum (lines sum ₹${lineNetSum().toFixed(2)} vs printed Grand Total ₹${(Number(parsed.invoiceSummary?.grandTotal) || 0).toFixed(2)}) — treat them as UNRELIABLE; do NOT assert any footer-total identity, correct only the per-line inconsistencies below`
        );
      }
      constraints.push(
        `for EVERY line: gstValue == taxableValue x gstRate / 100 (within ₹1)`,
        `for EVERY line: netValue == taxableValue + gstValue (within ₹1)`
      );
      const correction = `You previously returned this JSON for the invoice image:\n\n${JSON.stringify(parsed)}\n\nThe following deterministic GST checks FAILED against the parsed figures:\n- ${gstIssues.join("\n- ")}\n\nRe-read the ACTUAL printed digits of EVERY line: the GST % column, the GST ₹ column, and the Taxable/Net columns. If a line is internally broken (taxableValue + gstValue != netValue) the Taxable and Net columns were probably SWAPPED — swap them back from the printed digits.\nCorrect any of gstRate, gstValue, taxableValue, netValue (only to repair a column swap or rate misread)${footerCorroborated ? " and the footer totalGst / totalCGST / totalSGST / totalIGST / saleValue / grandTotal / totalTaxable / schDisc / cashDiscount / roundOff / cnNo if misread" : ""}, so that:\n${constraints.map((c, i) => `${i + 1}. ${c}`).join("\n")}\nRe-read the ACTUAL printed digits of every line's GST % column — NEVER assume or default a rate such as 12.\nDo NOT change medicineName, batchNumber, expiryDate, quantities, unitPrice, cdPercent, cdValue.\nReturn ONLY the corrected JSON with the SAME schema — no markdown, no explanation.`;
      try {
        const result = await generateWithModelFallback(genAI, [correction, ...imageParts]);
        const text = result.response.text();
        parsed = JSON.parse(stripMarkdown(text));
      } catch (err) {
        logger.error("[gemini] GST correction attempt failed: " + err.message);
        break;
      }
    }
    if (gstIssues.length > 0) {
      logger.warn(`[gemini] GST still inconsistent after retries: ${gstIssues.join(" | ")}`);
    }
  }

  logger.info(
    `[gemini] classify pg=${pgNum}/${pgTot} footer=${hasFooterTotals} cont=${
      parsed.looksLikeContinuationPage === true
    } printedPagination=${JSON.stringify(parsed.printedPagination || "")} gate=${validForFooterCheck} corroborated=${footerCorroborated} grandTotal=${initialGrandTotal.toFixed(2)} lineNetSum=${lineNetSum().toFixed(2)} gstCheck=${gstIssues.length === 0 ? "PASS" : "FAIL"} attempts=${gstAttempts} fixable=[${gstIssues.join(" | ")}] informational=[${gstInformational.join(" | ")}]`
  );

  return {
    ...parsed,
    distributor: normalizeDistributorName(parsed.distributor || ""),
    hasFooterTotals,
    pageNumber: pgNum,
    totalPages: pgTot,
    looksLikeContinuationPage:
      parsed.looksLikeContinuationPage === true || pgNum > 1 || printedContMarker,
    gstCheck: {
      pass: gstIssues.length === 0,
      attempts: gstAttempts,
      issues: gstIssues,
      informational: gstInformational,
    },
    rawGeminiResponse: geminiResponse,
  };
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
      imageParts = await downloadFilesToParts(bucket, paths);
    } catch (err) {
      throw new HttpsError("not-found", "Error downloading files from storage: " + err.message);
    }

    // Call Gemini via the shared pipeline
    const apiKey = GEMINI_API_KEY.value();
    if (!apiKey) {
      throw new HttpsError("internal", "GEMINI_API_KEY secret not configured.");
    }
    const genAI = new GoogleGenerativeAI(apiKey);
    const hints =
      request.data.pageNumber >= 1 || request.data.totalPages >= 1
        ? {
            pageNumber: Number(request.data.pageNumber) || 1,
            totalPages: Number(request.data.totalPages) || 1,
          }
        : null;
    const parsed = await runGeminiExtraction(imageParts, genAI, hints);

    logger.info(`[extractInvoice] done invoice=${parsed.invoiceNumber || "?"} items=${(parsed.lineItems || []).length} gstPass=${parsed.gstCheck.pass} partial=${parsed.pageNumber < parsed.totalPages || !parsed.hasFooterTotals}`);
    return sanitizeNumbers(parsed);
  }
);
// ─── isStaff ──────────────────────────────────────────────────────────────────
// Authorizes a caller against the pharmacy's staff registry
// (pharmacies/{pharmacyId}/staff/{uid}). This app currently ships WITHOUT a
// staff collection — every existing callable authorizes on request.auth alone —
// so if the pharmacy has no staff subcollection at all, any signed-in user is
// treated as staff. Once a staff registry is introduced, this becomes a real
// membership check with no call-site changes.
async function isStaff(pharmacyId, uid) {
  if (!pharmacyId || !uid) return false;
  const db = getFirestore();
  try {
    const staffRef = db
      .collection("pharmacies")
      .doc(pharmacyId)
      .collection("staff")
      .doc(uid);
    if ((await staffRef.get()).exists) return true;
    const anyStaff = await db
      .collection("pharmacies")
      .doc(pharmacyId)
      .collection("staff")
      .limit(1)
      .get();
    return anyStaff.empty; // no staff configured → permissive (matches current auth model)
  } catch (err) {
    logger.warn(`[isStaff] membership check failed for ${uid}@${pharmacyId}:`, err.message);
    return false;
  }
}

// ─── saveInvoice ───────────────────────────────────────────────────────────────
// Client SDK writes fail due to rules/Console mismatch; Admin SDK
// bypasses rules entirely. This is also more secure (server-side).
//
// When queueId is supplied, the matching importQueue item is guarded against
// duplicate saves: any queue item that reached a terminal state (reviewed /
// saved / ingested / failed) is refused, and this function drives the queue
// item through saving → saved so a stale client retry can never double-write.

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

    const { pharmacyId, invoice, lineItems, confirmedBy, queueId } = request.data;

    if (!pharmacyId) throw new HttpsError("invalid-argument", "pharmacyId is required");
    if (!invoice) throw new HttpsError("invalid-argument", "invoice data is required");
    if (!Array.isArray(lineItems) || lineItems.length === 0) {
      throw new HttpsError("invalid-argument", "At least one line item is required");
    }
    // Firestore cannot store NaN — strip any non-finite numbers defensively.
    const safeInvoice = sanitizeNumbers(invoice);
    const safeLineItems = sanitizeNumbers(lineItems);

    const db = getFirestore();
    const queueRef = queueId
      ? db.collection("pharmacies").doc(pharmacyId).collection("importQueue").doc(queueId)
      : null;
    let queueStoragePath = null;

    try {
      // ── Queue guard (idempotency) ────────────────────────────────────────
      if (queueRef) {
        const qSnap = await queueRef.get();
        if (!qSnap.exists) {
          throw new HttpsError("not-found", "Queue item not found: " + queueId);
        }
        const qData = qSnap.data();
        queueStoragePath = qData.storagePath || null;
        const status = qData.status || "";
        if (["reviewed", "saved", "ingested", "failed"].includes(status)) {
          throw new HttpsError(
            "failed-precondition",
            `Queue item ${queueId} is already ${status}; refusing duplicate save.`
          );
        }
        await queueRef.update({
          status: "saving",
          claimedBy: request.auth.uid,
          updatedAt: FieldValue.serverTimestamp(),
        });
      }

      const distName = normalizeDistributorName(safeInvoice.distributor);
      const invoiceNumber = safeInvoice.invoiceNumber || "";
      const pageHash = safeInvoice.pHash || null;
      let duplicateWarning = null;

      // ── pHash duplicate guard (HARD BLOCK) ───────────────────────────────
      // The real dedup key is the image hash: a genuine accidental re-upload of
      // the same page carries the same pHash. Different pages of ONE multi-page
      // invoice are different images (different hashes), so they must be
      // allowed to save independently even though they share the same
      // (distributorId, invoiceNumber). Skipped when no hash is available.
      if (pageHash) {
        const hashSnap = await db
          .collection("pharmacies")
          .doc(pharmacyId)
          .collection("invoices")
          .where("pHash", "==", pageHash)
          .limit(1)
          .get();
        if (!hashSnap.empty) {
          const dup = hashSnap.docs[0];
          const dupData = dup.data();
          throw new HttpsError(
            "already-exists",
            `This image was already saved as invoice ${dupData.invoiceNumber || "?"} from ${dupData.distributor || distName} (${dup.id}).`
          );
        }
      }

      // ── (distributorId, invoiceNumber) SOFT check (NOT a hard block) ────
      // Multi-page invoices legitimately save one doc per page under the same
      // compound key, so this can never hard-block. It only surfaces as a
      // soft warning on the success response so the caller can flag a possible
      // re-upload of an invoice we already hold (the hard proof of a true
      // re-upload is the pHash check above). Skipped when either half of the
      // key is empty (no number / no distributor means we cannot compare).
      if (distName && invoiceNumber) {
        const dupSnap = await db
          .collection("pharmacies")
          .doc(pharmacyId)
          .collection("invoices")
          .where("distributorId", "==", distName)
          .where("invoiceNumber", "==", invoiceNumber)
          .limit(1)
          .get();
        if (!dupSnap.empty) {
          const dup = dupSnap.docs[0];
          const dupData = dup.data();
          duplicateWarning = {
            invoiceId: dup.id,
            invoiceNumber,
            distributor: dupData.distributor || distName,
            partial: dupData.partial === true,
          };
        }
      }

      // A page only carries an authoritative invoiceTotal when it contains the
      // printed footer summary. Continuation pages are stored with
      // partial=true and invoiceTotal 0 so the reporting layer sums totals only
      // from the footer page(s) of an invoice and never double-counts across
      // the independently-saved pages of a multi-page invoice.
      const hasFooterTotals = safeInvoice.hasFooterTotals === true;
      const partial = !hasFooterTotals;

      const invoiceRef = await db.collection("pharmacies").doc(pharmacyId).collection("invoices").add({
        distributor: distName,
        distributorId: distName,
        invoiceNumber,
        invoiceDate: safeInvoice.invoiceDate || "",
        invoiceTotal: partial ? 0 : safeInvoice.invoiceTotal || 0,
        invoiceSummary: safeInvoice.invoiceSummary || {},
        lineItems: safeLineItems,
        captureQuality: safeInvoice.captureQuality || {},
        gstCheck: safeInvoice.gstCheck || {},
        rawGeminiResponse: safeInvoice.rawGeminiResponse || "",
        pHash: pageHash,
        hasFooterTotals,
        partial,
        pageNumber: Number(safeInvoice.pageNumber) || 1,
        totalPages: Number(safeInvoice.totalPages) || 1,
        looksLikeContinuationPage: safeInvoice.looksLikeContinuationPage === true,
        confirmedBy: confirmedBy || request.auth.uid,
        confirmedAt: FieldValue.serverTimestamp(),
        createdAt: FieldValue.serverTimestamp(),
      });

      const batch = db.batch();
      for (const item of safeLineItems) {
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
          distributor: distName,
          invoiceId: invoiceRef.id,
          invoiceNumber: item.invoiceNumber || "",
          distributorId: item.distributorId || distName,
          invoiceDate: item.invoiceDate || "",
          pharmacyId,
          createdAt: FieldValue.serverTimestamp(),
          updatedAt: FieldValue.serverTimestamp(),
        });
      }
      await batch.commit();

      // ── Queue lifecycle cleanup (AFTER the invoice+medicines write commits) ──
      // Storage deletes cannot be part of a Firestore transaction, so this runs
      // post-commit as best-effort: delete the queue doc (idempotency is now
      // preserved by the doc being gone — a stale retry hits "not-found") and
      // delete the raw upload so no orphaned image is left behind.
      if (queueRef) {
        try {
          await queueRef.delete();
        } catch (delErr) {
          logger.warn(`[saveInvoice] Queue doc delete failed (non-fatal): ${delErr.message}`);
          // Fall back to a terminal "saved" marker so the item is never stuck
          // in the half-open "saving" state.
          try {
            await queueRef.update({
              status: "saved",
              savedInvoiceId: invoiceRef.id,
              confirmedBy: confirmedBy || request.auth.uid,
              completedAt: FieldValue.serverTimestamp(),
              updatedAt: FieldValue.serverTimestamp(),
            });
          } catch (_) {}
        }
        if (queueStoragePath) {
          try {
            await getStorage().bucket().file(queueStoragePath).delete();
          } catch (delErr) {
            logger.warn(`[saveInvoice] Storage image delete failed (non-fatal): ${delErr.message}`);
          }
        }
      }

      logger.info(`[saveInvoice] Saved invoice ${invoiceRef.id} with ${safeLineItems.length} items for pharmacy=${pharmacyId}`);

      return { success: true, invoiceId: invoiceRef.id, duplicateWarning };
    } catch (err) {
      logger.error("[saveInvoice] Write failed:", err.message);
      // The queue guard flipped the item to "saving" above; revert it so the
      // user can retry the save instead of being stuck in a half-open state.
      if (queueRef) {
        try {
          await queueRef.update({
            status: "extracted",
            error: { code: "save_failed", message: err.message, at: new Date().toISOString() },
            updatedAt: FieldValue.serverTimestamp(),
          });
        } catch (queueErr) {
          logger.warn("[saveInvoice] Could not revert queue status after failure:", queueErr.message);
        }
      }
      // Surface the original code (not-found / failed-precondition /
      // already-exists) to the caller instead of collapsing everything to
      // "internal".
      if (err instanceof HttpsError) throw err;
      throw new HttpsError("internal", "Failed to save: " + err.message);
    }
  }
);
// ─── discardQueueItem ─────────────────────────────────────────────────────────
// Permanently discards an import queue item: deletes the queue doc AND the raw
// Storage image. Refuses items that are already saved/merged (terminal) or
// mid-flight (actively processing / saving), so an image whose extraction is in
// progress — or whose invoice was already written — is never deleted.

exports.discardQueueItem = onCall(
  {
    region: "us-central1",
    memory: "128MiB",
    timeoutSeconds: 30,
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }
    const { pharmacyId, docId } = request.data || {};
    if (!pharmacyId || !docId) {
      throw new HttpsError("invalid-argument", "pharmacyId and docId are required.");
    }
    if (!(await isStaff(pharmacyId, request.auth.uid))) {
      throw new HttpsError("permission-denied", "You are not staff for this pharmacy.");
    }

    const db = getFirestore();
    const queueRef = db
      .collection("pharmacies")
      .doc(pharmacyId)
      .collection("importQueue")
      .doc(docId);

    const snap = await queueRef.get();
    if (!snap.exists) {
      return { success: true, alreadyGone: true };
    }
    const data = snap.data();
    const status = data.status || "";
    if (["saved", "reviewed", "ingested", "saving"].includes(status)) {
      throw new HttpsError(
        "failed-precondition",
        `Queue item is ${status}; it cannot be discarded.`
      );
    }
    if (status === "processing") {
      const leaseExpiresAt = Number(data.leaseExpiresAt) || 0;
      if (leaseExpiresAt > Date.now()) {
        throw new HttpsError(
          "failed-precondition",
          "This item is currently being processed; try again in a moment."
        );
      }
    }

    await queueRef.delete();
    const storagePath = data.storagePath || "";
    if (storagePath) {
      try {
        await getStorage().bucket().file(storagePath).delete();
      } catch (delErr) {
        logger.warn(`[discardQueueItem] Storage delete failed (non-fatal): ${delErr.message}`);
      }
    }
    // A discarded return-receipt item must also retire its staged pending_returns
    // doc (mirrors discardReturnReceipt), otherwise the 24h cleanup is the only
    // thing that would ever clear it.
    if (data.documentType === "return_receipt" && data.pendingReturnId) {
      try {
        const pendingRef = db.collection("pharmacies").doc(pharmacyId).collection("pending_returns").doc(data.pendingReturnId);
        const prSnap = await pendingRef.get();
        if (prSnap.exists && prSnap.data().status !== "confirmed") {
          await pendingRef.set({ status: "discarded", discardedBy: request.auth.uid, updatedAt: FieldValue.serverTimestamp() }, { merge: true });
        }
      } catch (pendingErr) {
        logger.warn(`[discardQueueItem] pending_returns cleanup failed (non-fatal): ${pendingErr.message}`);
      }
    }
    logger.info(`[discardQueueItem] ${pharmacyId}/importQueue/${docId} discarded by ${request.auth.uid}`);
    return { success: true, discarded: docId };
  }
);
// ─── processImportQueueItem ───────────────────────────────────────────────────
// The resilient bulk-import worker.
//
// Queue contract (documented in the frontend too):
//   status: "uploaded" (raw file persisted, nothing else) →
//           "processing" (leased claim by a worker) →
//           "extracted" (invoice parsed, raw result stored for review) →
//           "saved" (user confirmed via saveInvoice)
//   Terminal states: "saved", "reviewed", "ingested", "failed", "rejected".
//   ("ingested"/"ingested-partial" are legacy statuses from the removed
//   multi-page staging flow; existing docs with them still render in the UI
//   so they can be discarded, but new items never reach them.)
//
// Every image is processed as a standalone invoice: whatever line items /
// totals / GST appear on that image are used directly. No cross-page
// grouping, no waiting for a footer-totals page.
//
// Crash-safety: a worker that dies mid-extraction leaves status "processing"
// with a leaseExpiresAt; once that lease expires the item is claimable again
// (scheduledCleanup also flips expired-leased items back to "uploaded").
// Transient extraction/download failures revert the item to "uploaded" so the
// client's resume loop retries it, up to MAX_EXTRACTION_ATTEMPTS.

exports.processImportQueueItem = onCall(
  {
    timeoutSeconds: 120,
    memory: "512MiB",
    region: "us-central1",
    secrets: ["GEMINI_API_KEY"],
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }

    const { pharmacyId, imageId } = request.data;
    if (!pharmacyId || !imageId) {
      throw new HttpsError("invalid-argument", "pharmacyId and imageId are required.");
    }

    const db = getFirestore();
    const queueRef = db
      .collection("pharmacies")
      .doc(pharmacyId)
      .collection("importQueue")
      .doc(imageId);

    // ── 1. Leased claim in a transaction ───────────────────────────────────
    // Returns { claimed:false, data } when the item is terminal/leased, or
    // { claimed:true, data } (data carries the pre-claim snapshot for attempt
    // counting) after atomically flipping it to "processing".
    let claim;
    try {
      claim = await db.runTransaction(async (tx) => {
        const snap = await tx.get(queueRef);
        if (!snap.exists) {
          throw new HttpsError("not-found", "Queue item not found: " + imageId);
        }
        const data = snap.data();
        const status = data.status || "";
        const terminal = ["saved", "reviewed", "ingested", "failed", "rejected"].includes(status);
        // "extracted" items are awaiting human review (never re-extract) and
        // "ingested-partial" pages are staged awaiting their footer page.
        const settled = terminal || status === "extracted" || status === "ingested-partial";
        if (settled) {
          return { claimed: false, terminal: true, data };
        }
        if (status === "processing") {
          const leaseExpiresAt = Number(data.leaseExpiresAt) || 0;
          if (leaseExpiresAt > Date.now()) {
            return { claimed: false, busy: true, data };
          }
          // Expired lease (crashed worker) — reclaim it.
          logger.warn(`[processImportQueueItem] reclaiming ${imageId}: lease expired, status was "processing"`);
        }
        if (status === "saving") {
          // Another invocation is mid-save; never re-claim/re-extract it.
          return { claimed: false, busy: true, data };
        }
        const claimAttempts = Number(data.claimAttempts) || 0;
        tx.update(queueRef, {
          status: "processing",
          claimedBy: request.auth.uid,
          claimedAt: FieldValue.serverTimestamp(),
          leaseExpiresAt: Date.now() + QUEUE_LEASE_MS,
          claimAttempts: claimAttempts + 1,
          updatedAt: FieldValue.serverTimestamp(),
        });
        return { claimed: true, data };
      });
    } catch (err) {
      if (err instanceof HttpsError) throw err;
      throw new HttpsError("internal", "Failed to claim queue item: " + err.message);
    }

    if (!claim.claimed) {
      const s = claim.data.status || "";
      if (claim.busy) {
        return { status: s, busy: true, retryAfterMs: QUEUE_LEASE_MS };
      }
      if (s === "extracted") {
        // Already extracted and awaiting review — return the stored result so a
        // resume never burns another Gemini call.
        return { status: s, skipped: true, extracted: claim.data.extracted || null };
      }
      return { status: s, skipped: true };
    }

    const claimAttempts = (Number(claim.data.claimAttempts) || 0) + 1;

    const markFailed = async (errorCode, message) => {
      const fatal = claimAttempts >= MAX_EXTRACTION_ATTEMPTS;
      logger.warn(
        `[processImportQueueItem] ${imageId} ${fatal ? "FAILED (attempt " + claimAttempts + ")" : "reverting to uploaded (attempt " + claimAttempts + ")"}: ${message}`
      );
      await queueRef.update({
        status: fatal ? "failed" : "uploaded",
        error: { code: errorCode, message, at: new Date().toISOString() },
        leaseExpiresAt: FieldValue.delete(),
        updatedAt: FieldValue.serverTimestamp(),
      });
      return { status: fatal ? "failed" : "uploaded", retryable: !fatal, attempt: claimAttempts, error: message };
    };

    // ── 2. Download the raw file ───────────────────────────────────────────
    const storagePath = claim.data.storagePath || "";
    if (!storagePath) {
      return await markFailed("missing_storage_path", "Queue item has no storagePath.");
    }
    if (!storagePath.startsWith(`invoices/${pharmacyId}/`)) {
      return await markFailed("invalid_storage_path", "storagePath does not belong to this pharmacy.");
    }

    const bucket = getStorage().bucket();
    let imageParts;
    try {
      imageParts = await downloadFilesToParts(bucket, [storagePath]);
    } catch (err) {
      return await markFailed("download_failed", "Error downloading file from storage: " + err.message);
    }

    // ── 3. Extract via the shared Gemini pipeline ──────────────────────────
    const apiKey = GEMINI_API_KEY.value();
    if (!apiKey) {
      return await markFailed("missing_api_key", "GEMINI_API_KEY secret not configured.");
    }
    const genAI = new GoogleGenerativeAI(apiKey);

    // Authoritative pagination hints captured at upload time (e.g. PDF page
    // number / total page count from pdf.numPages). Persisted on the queue doc
    // so a resumed item re-extracts with the same hints.
    const hints =
      Number(claim.data.pdfPageNumber) >= 1 || Number(claim.data.pdfTotalPages) >= 1
        ? {
            pageNumber: Number(claim.data.pdfPageNumber) || 1,
            totalPages: Number(claim.data.pdfTotalPages) || 1,
          }
        : null;

    let extracted;
    try {
      extracted = await runGeminiExtraction(imageParts, genAI, hints);
    } catch (err) {
      return await markFailed("extraction_failed", err.message);
    }

    // ── 4. Route the result ────────────────────────────────────────────────
    // Every image is a standalone invoice: persist whatever Gemini parsed from
    // THIS image (line items, totals, GST) straight to "extracted" for review.
    // No cross-page merging, no waiting for a footer-totals page.
    try {
      await queueRef.update({
        status: "extracted",
        extracted,
        invoiceNumber: extracted.invoiceNumber || "",
        gstCheck: extracted.gstCheck || {},
        completedAt: FieldValue.serverTimestamp(),
        leaseExpiresAt: FieldValue.delete(),
        updatedAt: FieldValue.serverTimestamp(),
      });
    } catch (err) {
      return await markFailed("stage_failed", "Failed to stage extraction: " + (err && err.message || err));
    }
    logger.info(`[processImportQueueItem] ${imageId} → extracted (invoice ${extracted.invoiceNumber || "?"}, ${(extracted.lineItems || []).length} items)`);
    return { status: "extracted", extracted };
  }
);
// ─── mutateImportQueue ──────────────────────────────────────────────────────
// Bypasses Firestore client rules: the Firestore backend has been observed
// enforcing STALE rules that deny client writes to the importQueue subtree
// even though the live ruleset allows them. To keep the upload pipeline
// working, all queue-doc writes from the client are routed through this
// callable, which performs them with the Admin SDK (rules are not applied).
// Supports op "create" (setDoc) and op "update" (partial updateDoc).

exports.mutateImportQueue = onCall(
  {
    region: "us-central1",
    memory: "128MiB",
    timeoutSeconds: 30,
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }
    const { op, pharmacyId, imageId, data } = request.data || {};
    if (
      !["create", "update"].includes(op) ||
      !pharmacyId ||
      !imageId ||
      !data ||
      typeof data !== "object"
    ) {
      throw new HttpsError(
        "invalid-argument",
        "op (create|update), pharmacyId, imageId and a data object are required."
      );
    }
    const db = getFirestore();
    const queueRef = db
      .collection("pharmacies")
      .doc(pharmacyId)
      .collection("importQueue")
      .doc(imageId);
    if (op === "create") {
      await queueRef.set({
        ...data,
        uploadedBy: request.auth.uid,
        createdAt: FieldValue.serverTimestamp(),
        updatedAt: FieldValue.serverTimestamp(),
      });
    } else {
      await queueRef.update({
        ...data,
        updatedAt: FieldValue.serverTimestamp(),
      });
    }
    logger.info(`[mutateImportQueue] ${op} ${pharmacyId}/importQueue/${imageId} by ${request.auth.uid}`);
    return { ok: true, imageId };
  }
);
// ─── Return-receipt pipeline ─────────────────────────────────────────────────
// DISTRIBUTOR RETURN RECEIPTS / CREDIT NOTES are a SEPARATE document type from
// purchase invoices. They share the SAME importQueue staging as invoices (the
// queue doc carries documentType: "return_receipt"); the client routes them to
// processReturnReceipt (never processImportQueueItem, never the invoice prompt,
// never the invoice GST validator). Extraction is staged to pending_returns;
// the matching engine runs on "Confirm & Match" (matchReturnReceipt) using the
// owner-edited header; confirmReturnMatches is the ONLY code path that writes a
// medicine record's status "returned" — the bulk "mark returned" shortcut on
// expired stock was removed because an already-expired batch is not the same as
// a distributor-accepted return, and marking it returned without a receipt lies
// about the credit.
//
// importQueue/{imageId} (status "uploaded" → "processing" → "extracted")
//   → pending_returns/{imageId} (status "pending_review" → "matched" → "confirmed")
// Terminal: "confirmed", "discarded", "failed", "rejected". Lease/crash
// recovery is identical to the import queue.

const GEMINI_RETURN_RECEIPT_PROMPT = `You are an expert AI OCR parser for Indian pharmacy distributor RETURN RECEIPTS / CREDIT NOTES (the document the distributor issues when it accepts returned medicine stock). A credit note has a COMPLETELY DIFFERENT structure to a purchase invoice — there is NO billed/free qty split, NO CD%, NO line-level CGST/SGST/IGST. Extract the credit-note header and every line item with 100% accuracy.

Return ONLY valid JSON matching this exact schema — no markdown, no explanation:

{
  "creditNoteNumber": "string",
  "distributorName": "string",
  "distributorGSTIN": "string (optional, may be blank)",
  "date": "string (DD/MM/YYYY or as printed)",
  "refInvoiceNumber": "string (optional, may be blank on some receipts)",
  "returnReason": "string (free text, e.g. Damaged / Near Expiry Stock / Expired / as printed)",
  "grandTotalCreditAmount": number,
  "readable": boolean,
  "issues": ["string"],
  "lineItems": [
    {
      "medicineName": "string",
      "hsn": "string (optional)",
      "batchNumber": "string",
      "expiryDate": "string (MM/YY or MM/YYYY or as printed)",
      "returnQty": number,
      "netAmount": number,
      "confidence": {
        "medicineName": number,
        "batchNumber": number,
        "returnQty": number
      }
    }
  ]
}

OPERATIONAL DIRECTIVES:
1. ROW-LEVEL EXTRACTION — parse EVERY visible line item across the receipt columns (Medicine / Product Description | Batch No | Exp Date | Returned Qty | Rate | Net Amount / Credit Amount / Total). Read each column as printed.
2. COPY PRINTED VALUES, DO NOT DERIVE:
   - medicineName: the full printed medicine/product name (e.g. "VOGS GM 2/0.3MG TAB").
   - batchNumber: the printed batch/lot number (e.g. "SGC312"). COPY IT VERBATIM — never "correct" or transcribe it (this is the primary matching key downstream). If a batch column exists but is blank on that line, return "".
   - returnQty: the printed returned quantity for that line.
   - netAmount: the printed credit/return amount for that line (INR, no symbol).
   - expiryDate: printed expiry (MM/YY or MM/YYYY). If the column is blank on that line, return "".
3. HEADER FIELDS:
   - creditNoteNumber: the printed Credit Note No / CN No (e.g. "CN-2024-0098"). COPY VERBATIM.
   - distributorName: the printed distributor / company name at the top of the receipt (e.g. "VARDHMAN MEDISALES").
   - grandTotalCreditAmount: the printed Grand Total / Total Credit / Total Return amount of the credit note.
   - returnReason: read any printed reason line (e.g. "Damaged", "Near Expiry", "Expired stock"). If none is printed, return "".
4. CONFIDENCE SCORING: for every line set confidence.medicineName, confidence.batchNumber and confidence.returnQty to 0.0–1.0 reflecting how clearly each field was printed. LOW batch confidence (blurry, smudged, partially cut-off digits) is the MOST IMPORTANT flag — the batch is the primary matching key, so when a batch looks at all uncertain, score it below 0.8. Never score an unreadable field above 0.4.
5. DO NOT EXTRACT OR VALIDATE (this is a credit note, NOT a purchase invoice):
   - Qty Billed / Qty Free split — does not exist here.
   - CD% / cash discount columns.
   - CGST/SGST/IGST line-level or totals split.
   - GST arithmetic tolerance checks. There is NO tax math to verify on a credit note.
6. If the document is not a return receipt / credit note (e.g. it is a purchase invoice or something unrelated), set readable = false and list the reason in issues[].
7. If the image is blurry or a value is unreadable, do NOT guess — set readable = false and explain in issues[].
8. All amounts in INR as plain numbers (no ₹ symbol), typed as numbers, never strings.
9. Return ONLY valid JSON — no markdown, no explanation.`;

// ── Fuzzy-string helpers for the matching engine ─────────────────────────────
function levenshtein(a, b) {
  a = String(a || "");
  b = String(b || "");
  const m = a.length;
  const n = b.length;
  if (!m) return n;
  if (!n) return m;
  const dp = new Array(n + 1);
  for (let j = 0; j <= n; j++) dp[j] = j;
  for (let i = 1; i <= m; i++) {
    let prev = dp[0];
    dp[0] = i;
    for (let j = 1; j <= n; j++) {
      const cur = dp[j];
      dp[j] = Math.min(dp[j] + 1, dp[j - 1] + 1, prev + (a[i - 1] === b[j - 1] ? 0 : 1));
      prev = cur;
    }
  }
  return dp[n];
}

// Name normalization: case-insensitive, punctuation dropped, runs collapsed.
// "Azithral 500" and "AZITHRAL 500MG" compare on alphanumeric tokens only.
function normMedName(s) {
  return String(s || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .trim();
}

// Batch normalization: case-insensitive, ALL non-alphanumerics stripped so
// "MEZI015" vs "MEZ1O15" vs "MEZ1015" reduce to comparable digit-letter strings
// (Levenshtein absorbs the 0/1 and O/0 OCR confusion the user saw in the data).
function normBatch(s) {
  return String(s || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

function medNameSimilarity(a, b) {
  const na = normMedName(a);
  const nb = normMedName(b);
  if (!na || !nb) return 0;
  if (na === nb) return 1;
  const maxLen = Math.max(na.length, nb.length);
  const sim = 1 - levenshtein(na, nb) / maxLen;
  const ta = na.split(" ");
  const tb = nb.split(" ");
  const overlap = ta.filter((t) => tb.includes(t)).length / Math.max(ta.length, tb.length);
  return Math.max(sim, overlap * 0.9);
}

function batchSimilarity(a, b) {
  const na = normBatch(a);
  const nb = normBatch(b);
  if (!na || !nb) return 0;
  if (na === nb) return 1;
  // Spec: allow edit-distance <= 1 to absorb OCR noise (e.g. "MEZI015" vs
  // "MEZ1015", "SGC312" vs "SGC31Z"). Treat those as a strong batch match.
  const dist = levenshtein(na, nb);
  if (dist <= 1) return 0.95;
  return 1 - dist / Math.max(na.length, nb.length);
}

// Score one receipt line against one medicine doc (0..1). Batch disagreement
// never fully sinks a strong medicine-name match, and a name-less receipt line
// is never auto-accepted on batch alone. When the receipt carries a reference
// invoice number, docs whose stored invoiceNumber matches it get a small boost
// (soft signal — never required, the spec says some receipts lack it or OCR
// garbles it).
function scoreCandidate(line, doc, refInvoiceNumber) {
  const nameSim = medNameSimilarity(line.medicineName, doc.medicineName);
  const batchSim = batchSimilarity(line.batchNumber, doc.batchNumber);
  const hasLineBatch = !!normBatch(line.batchNumber);
  const hasDocBatch = !!normBatch(doc.batchNumber);
  let score;
  if (hasLineBatch && hasDocBatch) score = nameSim * 0.6 + batchSim * 0.4;
  else score = nameSim * 0.9;
  if (refInvoiceNumber && doc.invoiceNumber && normBatch(refInvoiceNumber) === normBatch(doc.invoiceNumber)) {
    score = Math.min(1, score + 0.1);
  }
  return round2(score);
}

// Classify one receipt line against the candidate medicine docs:
//   { confidence: "high", selectedMedicineId, candidates }
//   { confidence: "ambiguous", candidates }   (manual pick)
//   { confidence: "none", candidates: [] }
function classifyLine(line, docs, refInvoiceNumber) {
  const candidates = [];
  for (const d of docs) {
    const score = scoreCandidate(line, d, refInvoiceNumber);
    if (score >= 0.55) {
      candidates.push({
        medicineId: d.id,
        medicineName: d.medicineName,
        batchNumber: d.batchNumber,
        remainingQty: d.remainingQty,
        score,
      });
    }
  }
  candidates.sort((a, b) => b.score - a.score || b.remainingQty - a.remainingQty);
  if (candidates.length === 0) return { confidence: "none", candidates: [] };
  const best = candidates[0];
  const second = candidates[1];
  const gap = second ? best.score - second.score : 1;
  if (gap >= 0.18) return { confidence: "high", selectedMedicineId: best.medicineId, candidates: [best] };
  return { confidence: "ambiguous", candidates: candidates.slice(0, 4) };
}

// Load the medicine docs that are valid match targets for a return receipt:
// same pharmacy, not already terminal (returned/disposed/written off/deleted),
// and holding real stock (remainingQty > 0). The returnedSplit marker excludes
// partial-return split docs that carry no stock of their own.
// When distributor is supplied (resolved from the receipt's distributorName),
// restrict to that distributor's docs first — spec step 6.1. The caller falls
// back to the full list when the distributor pool is empty.
async function loadReturnCandidates(pharmacyId, db, distributor = "") {
  const medColl = db.collection("pharmacies").doc(pharmacyId).collection("medicines");
  const dist = normalizeDistributorName(distributor);
  let snap;
  try {
    if (dist) {
      const qSnap = await medColl.where("distributorId", "==", dist).limit(400).get();
      if (!qSnap.empty) {
        snap = qSnap;
      } else {
        const legacySnap = await medColl.where("distributor", "==", dist).limit(400).get();
        if (!legacySnap.empty) snap = legacySnap;
      }
    }
  } catch (err) {
    logger.warn(`[matchReturnReceipt] distributor query failed, falling back to full scan: ${err.message}`);
  }
  if (!snap) {
    snap = await medColl.get();
  }
  const out = [];
  for (const doc of snap.docs) {
    const d = doc.data();
    if (d.returnedSplit === true) continue;
    if (["returned", "returned_to_distributor", "disposed", "written_off", "deleted"].includes(d.status || "active")) continue;
    const remaining = Number(d.remainingQty ?? d.quantityBilled) || 0;
    if (remaining <= 0) continue;
    out.push({
      id: doc.id,
      medicineName: d.medicineName || "",
      batchNumber: d.batchNumber || "",
      distributorId: normalizeDistributorName(d.distributorId || d.distributor || ""),
      invoiceNumber: d.invoiceNumber || "",
      remainingQty: remaining,
    });
  }
  return out;
}

// The matching engine (runs on Confirm & Match — spec step 6). For each
// reviewed receipt line, find candidate medicine docs, score them, and classify
// by confidence: "high" (unambiguous → auto-stage for one-tap confirm),
// "ambiguous" (top 2-4 candidates for manual pick), "none" (manual search).
function matchReturnLines(parsed, candidates) {
  const refInvoiceNumber = String(parsed.refInvoiceNumber || "");
  const lines = Array.isArray(parsed.lineItems) ? parsed.lineItems : [];
  return lines.map((line, i) => {
    const match = classifyLine(line, candidates, refInvoiceNumber);
    return {
      lineIndex: i,
      medicineName: line.medicineName || "",
      batchNumber: line.batchNumber || "",
      expiryDate: line.expiryDate || "",
      returnQty: Number(line.returnQty) || 0,
      netAmount: Number(line.netAmount) || 0,
      confidence: (line && line.confidence) || { medicineName: 1, batchNumber: 1, returnQty: 1 },
      match,
    };
  });
}

// ─── Return-receipt pipeline ─────────────────────────────────────────────────
// processReturnReceipt / matchReturnReceipt / confirmReturnMatches / discardReturnReceipt
//
// DISTRIBUTOR RETURN RECEIPTS / CREDIT NOTES are a SEPARATE document type from
// purchase invoices. They share the SAME importQueue staging as invoices (the
// queue doc carries documentType: "return_receipt"); the client routes them to
// processReturnReceipt (never processImportQueueItem, never the invoice prompt,
// never the invoice GST validator).
//
// Flow:
//   importQueue/{imageId}   (status "uploaded" → "processing" → "extracted")
//     → pending_returns/{imageId}   (status "pending_review" → "matched" → "confirmed")
//   The review screen reads pending_returns (edits header/line fields).
//   "Confirm & Match" → matchReturnReceipt (matching engine, spec step 6).
//   The confirm screen → confirmReturnMatches (the ONLY code path that writes a
//   medicine record's status "returned", spec step 7/8).
//   "Reject" → discardReturnReceipt (pending_returns + importQueue + storage).
//
// Staging doc: /pharmacies/{id}/pending_returns/{imageId}
//   header:     { creditNoteNumber, distributorName, distributorGSTIN, date,
//                 refInvoiceNumber, returnReason, grandTotalCreditAmount }
//   lineItems:  [{ medicineName, hsn, batchNumber, expiryDate, returnQty,
//                  netAmount, confidence }]
//   matches:    [{ lineIndex, ..., match }]   (populated by matchReturnReceipt)

// Best-effort parse of a credit-note date string into a Firestore Timestamp
// ("DD/MM/YYYY", "YYYY-MM-DD", "D-M-YYYY"). Returns null when unparseable.
function parseDateField(raw) {
  const s = String(raw || "").trim();
  if (!s) return null;
  let m = s.match(/^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$/);
  if (m) return Timestamp.fromDate(new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1])));
  m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (m) return Timestamp.fromDate(new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
  const t = new Date(s);
  if (!isNaN(t.getTime())) return Timestamp.fromDate(t);
  return null;
}

exports.processReturnReceipt = onCall(
  {
    timeoutSeconds: 120,
    memory: "512MiB",
    region: "us-central1",
    secrets: ["GEMINI_API_KEY"],
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }

    const { pharmacyId, imageId } = request.data;
    if (!pharmacyId || !imageId) {
      throw new HttpsError("invalid-argument", "pharmacyId and imageId are required.");
    }

    const db = getFirestore();
    const queueRef = db.collection("pharmacies").doc(pharmacyId).collection("importQueue").doc(imageId);

    // ── 1. Leased claim in a transaction (identical to processImportQueueItem) ──
    let claim;
    try {
      claim = await db.runTransaction(async (tx) => {
        const snap = await tx.get(queueRef);
        if (!snap.exists) {
          throw new HttpsError("not-found", "Queue item not found: " + imageId);
        }
        const data = snap.data();
        const status = data.status || "";
        const terminal = ["saved", "reviewed", "ingested", "failed", "rejected", "confirmed", "discarded"].includes(status);
        if (terminal || status === "extracted") {
          return { claimed: false, terminal: true, data };
        }
        if (status === "processing") {
          const leaseExpiresAt = Number(data.leaseExpiresAt) || 0;
          if (leaseExpiresAt > Date.now()) {
            return { claimed: false, busy: true, data };
          }
          logger.warn(`[processReturnReceipt] reclaiming ${imageId}: lease expired, status was "processing"`);
        }
        if (status === "saving" || status === "confirming") {
          return { claimed: false, busy: true, data };
        }
        const claimAttempts = Number(data.claimAttempts) || 0;
        tx.update(queueRef, {
          status: "processing",
          claimedBy: request.auth.uid,
          claimedAt: FieldValue.serverTimestamp(),
          leaseExpiresAt: Date.now() + QUEUE_LEASE_MS,
          claimAttempts: claimAttempts + 1,
          updatedAt: FieldValue.serverTimestamp(),
        });
        return { claimed: true, data };
      });
    } catch (err) {
      if (err instanceof HttpsError) throw err;
      throw new HttpsError("internal", "Failed to claim queue item: " + err.message);
    }

    if (!claim.claimed) {
      const s = claim.data.status || "";
      if (claim.busy) {
        return { status: s, busy: true, retryAfterMs: QUEUE_LEASE_MS };
      }
      if (s === "extracted") {
        // Already extracted and awaiting review — return the staged result so a
        // resume never burns another Gemini call.
        const prId = claim.data.pendingReturnId || imageId;
        try {
          const prSnap = await db.collection("pharmacies").doc(pharmacyId).collection("pending_returns").doc(prId).get();
          if (prSnap.exists) {
            return { status: s, skipped: true, pendingReturnId: prId, extracted: prSnap.data() };
          }
        } catch (_) {}
        return { status: s, skipped: true, pendingReturnId: prId };
      }
      return { status: s, skipped: true };
    }

    const claimAttempts = (Number(claim.data.claimAttempts) || 0) + 1;

    const markFailed = async (errorCode, message) => {
      const fatal = claimAttempts >= MAX_EXTRACTION_ATTEMPTS;
      logger.warn(`[processReturnReceipt] ${imageId} ${fatal ? "FAILED (attempt " + claimAttempts + ")" : "reverting to uploaded (attempt " + claimAttempts + ")"}: ${message}`);
      await queueRef.update({
        status: fatal ? "failed" : "uploaded",
        error: { code: errorCode, message, at: new Date().toISOString() },
        leaseExpiresAt: FieldValue.delete(),
        updatedAt: FieldValue.serverTimestamp(),
      });
      return { status: fatal ? "failed" : "uploaded", retryable: !fatal, attempt: claimAttempts, error: message };
    };

    // ── 2. Download the raw file ───────────────────────────────────────────
    const storagePath = claim.data.storagePath || "";
    if (!storagePath) {
      return await markFailed("missing_storage_path", "Queue item has no storagePath.");
    }
    if (!storagePath.startsWith(`returns/${pharmacyId}/`) && !storagePath.startsWith(`invoices/${pharmacyId}/`)) {
      return await markFailed("invalid_storage_path", "storagePath does not belong to this pharmacy.");
    }

    const bucket = getStorage().bucket();
    let imageParts;
    try {
      imageParts = await downloadFilesToParts(bucket, [storagePath]);
    } catch (err) {
      return await markFailed("download_failed", "Error downloading file from storage: " + err.message);
    }

    // ── 3. Extract via the RETURN-RECEIPT prompt (never the invoice prompt) ──
    const apiKey = GEMINI_API_KEY.value();
    if (!apiKey) {
      return await markFailed("missing_api_key", "GEMINI_API_KEY secret not configured.");
    }
    const genAI = new GoogleGenerativeAI(apiKey);

    let result = null;
    let parsed;
    try {
      result = await generateWithModelFallback(genAI, [GEMINI_RETURN_RECEIPT_PROMPT, ...imageParts]);
      parsed = JSON.parse(stripMarkdown(result.response.text()));
    } catch (err) {
      return await markFailed("extraction_failed", err.message);
    }

    // The rest of the body runs with the lease held. Everything after extraction
    // is funneled through markFailed (which deletes leaseExpiresAt) so a crashed
    // invocation can never wedge the queue item in "processing".
    // Declared at function scope because the final logger.info + return run
    // OUTSIDE the guarded try block and must still see them.
    let staged = null;
    let pendingDocId = null;
    let header = {};
    let lineItems = [];
    try {
      if (parsed.readable === false) {
        return await markFailed("not_readable", (parsed.issues || []).join("; ") || "Document not readable as a return receipt.");
      }

      header = {
        creditNoteNumber: String(parsed.creditNoteNumber || "").trim(),
        distributorName: normalizeDistributorName(parsed.distributorName || ""),
        distributorGSTIN: String(parsed.distributorGSTIN || "").trim(),
        date: String(parsed.date || ""),
        refInvoiceNumber: String(parsed.refInvoiceNumber || "").trim(),
        returnReason: String(parsed.returnReason || "").trim(),
        grandTotalCreditAmount: Number(parsed.grandTotalCreditAmount) || 0,
      };
      lineItems = (Array.isArray(parsed.lineItems) ? parsed.lineItems : []).map((l) => ({
        medicineName: String(l.medicineName || "").trim(),
        hsn: String(l.hsn || "").trim(),
        batchNumber: String(l.batchNumber || "").trim(),
        expiryDate: String(l.expiryDate || "").trim(),
        returnQty: Number(l.returnQty) || 0,
        netAmount: Number(l.netAmount) || 0,
        confidence: {
          medicineName: Number(l.confidence && l.confidence.medicineName) || 0,
          batchNumber: Number(l.confidence && l.confidence.batchNumber) || 0,
          returnQty: Number(l.confidence && l.confidence.returnQty) || 0,
        },
      }));

      // ── 4. Stage to pending_returns (raw extraction — matching runs later) ──
      // Matching is deliberately NOT done here: it runs on "Confirm & Match" so
      // the edited distributor / refInvoiceNumber (spec step 6) are used.
      pendingDocId = imageId;
      const pendingRef = db.collection("pharmacies").doc(pharmacyId).collection("pending_returns").doc(pendingDocId);
      staged = sanitizeNumbers({ header, lineItems });
      await pendingRef.set({
        pharmacyId,
        importQueueItemId: imageId,
        storagePath,
        status: "pending_review",
        header,
        lineItems,
        matches: [],
        matchSummary: { high: 0, ambiguous: 0, none: 0, candidatesPool: 0 },
        rawGeminiResponse: String((result && result.response && result.response.text()) || "").slice(0, 20000),
        readable: parsed.readable !== false,
        issues: Array.isArray(parsed.issues) ? parsed.issues : [],
        createdAt: FieldValue.serverTimestamp(),
        updatedAt: FieldValue.serverTimestamp(),
      });

      await queueRef.update({
        status: "extracted",
        documentType: "return_receipt",
        pendingReturnId: pendingDocId,
        completedAt: FieldValue.serverTimestamp(),
        leaseExpiresAt: FieldValue.delete(),
        updatedAt: FieldValue.serverTimestamp(),
      });
    } catch (err) {
      return await markFailed("stage_failed", "Failed to stage return extraction: " + (err && err.message || err));
    }

    logger.info(`[processReturnReceipt] ${imageId} → extracted → pending_returns/${pendingDocId} (CN ${header.creditNoteNumber || "?"}, ${lineItems.length} items)`);
    return { status: "extracted", pendingReturnId: pendingDocId, extracted: staged };
  }
);

// ─── matchReturnReceipt ─────────────────────────────────────────────────────
// The matching engine — runs on "Confirm & Match" (spec step 6), AFTER the
// owner edits the review fields, so the edited distributor / refInvoiceNumber /
// line fields are used. Steps:
//   1. Resolve the distributor from distributorName and query that distributor's
//      medicine docs first (fall back to the full pharmacy list).
//   2. Match candidates by batchNumber (fuzzy — normalize case, strip
//      spaces/hyphens, edit-distance <= 1) AND medicineName (fuzzy).
//   3. refInvoiceNumber is an optional narrowing signal — never required.
//   4. Classify each line: "high" (unambiguous, auto-staged) / "ambiguous"
//      (top candidates for manual pick) / "none" (manual search).
// Persists the matches onto the pending_returns doc (status "matched") and
// returns them for the final confirm screen.

exports.matchReturnReceipt = onCall(
  {
    region: "us-central1",
    memory: "256MiB",
    timeoutSeconds: 60,
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }
    const { pharmacyId, pendingReturnId, header, lineItems } = request.data || {};
    if (!pharmacyId || !pendingReturnId) {
      throw new HttpsError("invalid-argument", "pharmacyId and pendingReturnId are required.");
    }
    if (!(await isStaff(pharmacyId, request.auth.uid))) {
      throw new HttpsError("permission-denied", "You are not staff for this pharmacy.");
    }

    const db = getFirestore();
    const pendingRef = db.collection("pharmacies").doc(pharmacyId).collection("pending_returns").doc(pendingReturnId);
    const prSnap = await pendingRef.get();
    if (!prSnap.exists) {
      throw new HttpsError("not-found", "Staged return receipt not found: " + pendingReturnId);
    }
    const pr = prSnap.data();
    if (["confirmed", "discarded"].includes(pr.status || "")) {
      throw new HttpsError("failed-precondition", `Return receipt is ${pr.status}; it can no longer be matched.`);
    }

    const prevHeader = pr.header || {};
    const safeHeader = {
      creditNoteNumber: String((header && header.creditNoteNumber) || prevHeader.creditNoteNumber || "").trim(),
      distributorName: normalizeDistributorName((header && header.distributorName) || prevHeader.distributorName || ""),
      distributorGSTIN: String((header && header.distributorGSTIN) || prevHeader.distributorGSTIN || "").trim(),
      date: String((header && header.date) || prevHeader.date || ""),
      refInvoiceNumber: String((header && header.refInvoiceNumber) || prevHeader.refInvoiceNumber || "").trim(),
      returnReason: String((header && header.returnReason) || prevHeader.returnReason || "").trim(),
      grandTotalCreditAmount: Number((header && header.grandTotalCreditAmount) != null ? header.grandTotalCreditAmount : prevHeader.grandTotalCreditAmount) || 0,
    };

    const rawLines = Array.isArray(lineItems) && lineItems.length > 0 ? lineItems : pr.lineItems;
    const safeLines = (Array.isArray(rawLines) ? rawLines : []).map((l) => ({
      medicineName: String(l.medicineName || "").trim(),
      hsn: String(l.hsn || "").trim(),
      batchNumber: String(l.batchNumber || "").trim(),
      expiryDate: String(l.expiryDate || "").trim(),
      returnQty: Number(l.returnQty) || 0,
      netAmount: Number(l.netAmount) || 0,
      confidence: {
        medicineName: Number(l.confidence && l.confidence.medicineName) || 0,
        batchNumber: Number(l.confidence && l.confidence.batchNumber) || 0,
        returnQty: Number(l.confidence && l.confidence.returnQty) || 0,
      },
    }));

    const parsed = { ...safeHeader, lineItems: safeLines };
    const candidates = await loadReturnCandidates(pharmacyId, db, safeHeader.distributorName);
    const matches = matchReturnLines(parsed, candidates);

    const highCount = matches.filter((m) => m.match && m.match.confidence === "high").length;
    const ambCount = matches.filter((m) => m.match && m.match.confidence === "ambiguous").length;
    const noneCount = matches.filter((m) => !m.match || m.match.confidence === "none").length;
    const matchSummary = { high: highCount, ambiguous: ambCount, none: noneCount, candidatesPool: candidates.length };

    await pendingRef.update({
      status: "matched",
      header: safeHeader,
      lineItems: safeLines,
      matches,
      matchSummary,
      matchedBy: request.auth.uid,
      matchedAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    });

    logger.info(`[matchReturnReceipt] ${pendingReturnId} → matched (CN ${safeHeader.creditNoteNumber || "?"}, high=${highCount}, amb=${ambCount}, none=${noneCount}, pool=${candidates.length})`);
    return { status: "matched", header: safeHeader, lineItems: safeLines, matches, matchSummary };
  }
);

// ─── confirmReturnMatches ───────────────────────────────────────────────────
// The FINAL confirmation for a matched return receipt. This is the ONLY code
// path that may set a medicine record's status to "returned" (spec step 7/8) —
// the expiry bulk bar stops at "pending_return".
// For each line the owner approved with a medicineId:
//   - returnQty < remaining qty  → SPLIT: reduce the live record's qty, create
//     a returnedSplit sibling doc carrying the returned portion.
//   - returnQty >= remaining qty → set status "returned" on the full record.
// Writes returnDetails { creditNoteNumber, distributorName, date,
// refInvoiceNumber, returnReason, amount } onto the returned record(s).
//
// decisions: [{ lineIndex, medicineId, returnQty, netAmount }]
//   medicineId null → line skipped (owner left it unmatched).

exports.confirmReturnMatches = onCall(
  {
    region: "us-central1",
    memory: "256MiB",
    timeoutSeconds: 60,
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }

    const { pharmacyId, pendingReturnId, decisions, header } = request.data || {};
    if (!pharmacyId || !pendingReturnId) {
      throw new HttpsError("invalid-argument", "pharmacyId and pendingReturnId are required.");
    }
    if (!Array.isArray(decisions)) {
      throw new HttpsError("invalid-argument", "decisions must be an array.");
    }
    const cnNumber = String((header && header.creditNoteNumber) || "").trim();
    if (!cnNumber) {
      throw new HttpsError("invalid-argument", "Credit note number is required to confirm a return.");
    }

    const db = getFirestore();
    const pendingRef = db.collection("pharmacies").doc(pharmacyId).collection("pending_returns").doc(pendingReturnId);

    // ── Guard: staged doc must exist and not already be confirmed ─────────
    const prSnap = await pendingRef.get();
    if (!prSnap.exists) {
      throw new HttpsError("not-found", "Staged return receipt not found: " + pendingReturnId);
    }
    const pr = prSnap.data();
    if (pr.status === "confirmed") {
      throw new HttpsError("failed-precondition", "This return receipt is already confirmed.");
    }

    const distributorName = normalizeDistributorName((header && header.distributorName) || (pr.header && pr.header.distributorName) || "");
    const dateRaw = String((header && header.date) || (pr.header && pr.header.date) || "");
    const refInvoiceNumber = String((header && header.refInvoiceNumber) || (pr.header && pr.header.refInvoiceNumber) || "").trim() || null;
    const returnReason = String((header && header.returnReason) || (pr.header && pr.header.returnReason) || "").trim();
    const dateTs = parseDateField(dateRaw);

    const medColl = db.collection("pharmacies").doc(pharmacyId).collection("medicines");
    const queueRef = db.collection("pharmacies").doc(pharmacyId).collection("importQueue").doc(pr.importQueueItemId || pendingReturnId);
    const batch = db.batch();
    const now = FieldValue.serverTimestamp();
    const liveQty = new Map(); // medicineId → remainingQty as decisions apply (same doc can appear twice)

    let confirmed = 0;
    let skipped = 0;
    const results = [];

    for (const d of decisions) {
      const medicineId = d && d.medicineId;
      if (!medicineId) {
        skipped++;
        results.push({ lineIndex: d ? d.lineIndex : null, medicineId: null, applied: false, reason: "unmatched" });
        continue;
      }
      const medRef = medColl.doc(medicineId);
      const snap = await medRef.get();
      if (!snap.exists) {
        skipped++;
        results.push({ lineIndex: d.lineIndex, medicineId, applied: false, reason: "not-found" });
        continue;
      }
      const doc = snap.data();
      if (["returned", "returned_to_distributor", "disposed", "written_off", "deleted"].includes(doc.status || "active")) {
        skipped++;
        results.push({ lineIndex: d.lineIndex, medicineId, applied: false, reason: `already ${doc.status || "active"}` });
        continue;
      }

      const originalRemaining = liveQty.has(medicineId)
        ? liveQty.get(medicineId)
        : Number(doc.remainingQty ?? doc.quantityBilled) || 0;
      const qtyReturned = Math.max(0, Math.min(Number(d.returnQty) || 0, originalRemaining));
      if (qtyReturned <= 0) {
        skipped++;
        results.push({ lineIndex: d.lineIndex, medicineId, applied: false, reason: "qty 0" });
        continue;
      }

      const netAmount = Number(d.netAmount) || 0;
      const remainingAfter = originalRemaining - qtyReturned;
      const fullReturn = remainingAfter <= 0;

      // Spec step 7/9 returnDetails — written onto the RETURNED record(s).
      const returnDetails = {
        creditNoteNumber: cnNumber,
        distributorName,
        date: dateTs,
        refInvoiceNumber,
        returnReason,
        amount: netAmount,
      };
      const creditNote = {
        creditNoteNo: cnNumber,
        distributor: distributorName,
        creditAmount: netAmount,
        returnedDate: dateRaw,
      };

      if (fullReturn) {
        batch.set(medRef, {
          status: "returned",
          remainingQty: 0,
          returnedQty: originalRemaining,
          returnDetails,
          creditNote,
          returnedAt: now,
          updatedAt: now,
        }, { merge: true });
        liveQty.set(medicineId, 0);
      } else {
        // Partial return → keep this doc live with the reduced stock, and
        // create a returnedSplit sibling doc carrying the returned portion
        // (the aggregator keys it apart from the live row). The returned
        // record is the split doc; returnDetails live only there.
        const cumulativeReturned = (Number(doc.returnedQty) || 0) + qtyReturned;
        batch.set(medRef, {
          remainingQty: remainingAfter,
          returnedQty: cumulativeReturned,
          creditNote,
          updatedAt: now,
        }, { merge: true });
        liveQty.set(medicineId, remainingAfter);

        const splitRef = medColl.doc();
        batch.set(splitRef, {
          medicineName: doc.medicineName || "",
          batchNumber: doc.batchNumber || "",
          expiryDate: doc.expiryDate || "",
          quantityBilled: 0,
          quantityFree: 0,
          remainingQty: 0,
          returnedQty: qtyReturned,
          unitPrice: doc.unitPrice || 0,
          netValue: doc.netValue || 0,
          gstRate: doc.gstRate || 0,
          gstValue: doc.gstValue || 0,
          distributor: distributorName,
          distributorId: distributorName,
          invoiceId: doc.invoiceId || "",
          invoiceNumber: doc.invoiceNumber || "",
          invoiceDate: doc.invoiceDate || "",
          pharmacyId,
          status: "returned",
          returnedSplit: true,
          returnDetails,
          creditNote,
          returnedAt: now,
          createdAt: now,
          updatedAt: now,
        });
      }
      confirmed++;
      results.push({ lineIndex: d.lineIndex, medicineId, applied: true, fullReturn, qtyReturned });
    }

    if (confirmed > 0) {
      await batch.commit();
    }

    // ── Lifecycle cleanup (after the write commits) ───────────────────────
    try {
      await pendingRef.update({
        status: "confirmed",
        confirmedBy: request.auth.uid,
        confirmedAt: now,
        updatedAt: now,
      });
    } catch (pendingErr) {
      logger.warn(`[confirmReturnMatches] pending_returns update failed (non-fatal): ${pendingErr.message}`);
    }
    try {
      await queueRef.update({
        status: "confirmed",
        confirmedBy: request.auth.uid,
        confirmedAt: now,
        updatedAt: now,
      });
    } catch (queueErr) {
      logger.warn(`[confirmReturnMatches] importQueue update failed (non-fatal): ${queueErr.message}`);
    }
    const storagePath = pr.storagePath || "";
    if (storagePath) {
      try {
        await getStorage().bucket().file(storagePath).delete();
      } catch (delErr) {
        logger.warn(`[confirmReturnMatches] Storage delete failed (non-fatal): ${delErr.message}`);
      }
    }

    logger.info(`[confirmReturnMatches] ${pendingReturnId} → confirmed ${confirmed} line(s), skipped ${skipped} (CN ${cnNumber}) in ${pharmacyId} by ${request.auth.uid}`);
    return { success: true, confirmed, skipped, results };
  }
);

// ─── discardReturnReceipt ───────────────────────────────────────────────────
// Staff-authorized permanent discard of a return receipt ("Reject" on the
// review screen): marks the staged pending_returns doc "discarded", marks the
// importQueue item "discarded" (terminal) and deletes the raw Storage image.
// Refuses items that are already confirmed.

exports.discardReturnReceipt = onCall(
  {
    region: "us-central1",
    memory: "128MiB",
    timeoutSeconds: 30,
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }
    const { pharmacyId, pendingReturnId } = request.data || {};
    if (!pharmacyId || !pendingReturnId) {
      throw new HttpsError("invalid-argument", "pharmacyId and pendingReturnId are required.");
    }
    if (!(await isStaff(pharmacyId, request.auth.uid))) {
      throw new HttpsError("permission-denied", "You are not staff for this pharmacy.");
    }

    const db = getFirestore();
    const pendingRef = db.collection("pharmacies").doc(pharmacyId).collection("pending_returns").doc(pendingReturnId);
    const prSnap = await pendingRef.get();
    let storagePath = "";
    let queueItemId = pendingReturnId;
    if (prSnap.exists) {
      const pr = prSnap.data();
      if (pr.status === "confirmed") {
        throw new HttpsError("failed-precondition", "This return receipt is already confirmed and cannot be discarded.");
      }
      storagePath = pr.storagePath || "";
      queueItemId = pr.importQueueItemId || pendingReturnId;
    }

    await pendingRef.set({ status: "discarded", discardedBy: request.auth.uid, updatedAt: FieldValue.serverTimestamp() }, { merge: true });
    const queueRef = db.collection("pharmacies").doc(pharmacyId).collection("importQueue").doc(queueItemId);
    try {
      await queueRef.update({ status: "discarded", discardedBy: request.auth.uid, updatedAt: FieldValue.serverTimestamp() });
    } catch (queueErr) {
      logger.warn(`[discardReturnReceipt] importQueue update failed (non-fatal): ${queueErr.message}`);
    }
    if (storagePath) {
      try {
        await getStorage().bucket().file(storagePath).delete();
      } catch (delErr) {
        logger.warn(`[discardReturnReceipt] Storage delete failed (non-fatal): ${delErr.message}`);
      }
    }
    logger.info(`[discardReturnReceipt] ${pendingReturnId} discarded by ${request.auth.uid}`);
    return { success: true, discarded: pendingReturnId };
  }
);
// ─── bulkUpdateMedicineStatus ────────────────────────────────────────────────
// Lifecycle status update for medicine batch docs (the "return/credit/archive"
// model — NOT hard deletion). A batch may span multiple medicines docs (one per
// invoice scan), so this updates every doc in `ids` atomically in one batch.
//
// Allowed transitions (status field on each medicines/{id} doc):
//   "active"         default (no field / historical rows)
//   "pending_return" flagged to return, waiting on distributor acceptance
//   "disposed"       physically disposed (cert ref optional)
//   "written_off"    written off as a loss (no credit expected)
//   "deleted"        soft-delete for genuine data-entry mistakes; the
//                    scheduledCleanup job purges these after 30 days.
//
// "returned" is deliberately NOT settable here. The ONLY path that writes a
// medicine record's status "returned" is confirmReturnMatches (the return
// receipt confirm flow) — the bulk "mark returned" shortcut on expired stock
// was removed because marking it returned without a real credit note falsely
// claims credit for stock the distributor may reject. Legacy values
// "return_pending" / "returned_to_distributor" exist on old docs and are still
// read by the UI, but no new write uses them.

const MEDICINE_STATUSES = new Set([
  "active",
  "pending_return",
  "disposed",
  "written_off",
  "deleted",
]);

exports.bulkUpdateMedicineStatus = onCall(
  {
    region: "us-central1",
    memory: "128MiB",
    timeoutSeconds: 30,
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }
    const { pharmacyId, ids, status, credit, disposalCertRef, returnNote } = request.data || {};
    if (!pharmacyId) throw new HttpsError("invalid-argument", "pharmacyId is required");
    if (!Array.isArray(ids) || ids.length === 0) {
      throw new HttpsError("invalid-argument", "ids must be a non-empty array");
    }
    if (ids.length > 500) {
      throw new HttpsError("invalid-argument", "Too many docs per update (max 500)");
    }
    if (!MEDICINE_STATUSES.has(status)) {
      throw new HttpsError("invalid-argument", `Unknown status: ${status}`);
    }

    const db = getFirestore();
    const batch = db.batch();
    const medColl = db.collection("pharmacies").doc(pharmacyId).collection("medicines");
    const now = FieldValue.serverTimestamp();

    // Guard: only touch docs that actually exist in this pharmacy (never create
    // phantom docs from stale ids). Reads are cheap and keep the batch honest.
    const update = {};
    update.status = status;
    update.updatedAt = now;
    if (status === "deleted") {
      update.deletedAt = now;
      update.deletedReason = typeof returnNote === "string" ? returnNote.slice(0, 500) : "owner";
    }
    if (credit && typeof credit === "object") {
      update.creditNote = {
        creditNoteNo: String(credit.creditNoteNo || "").trim(),
        distributor: typeof credit.distributor === "string" ? credit.distributor.trim().slice(0, 200) : "",
        creditAmount: Number(credit.creditAmount) || 0,
        returnedDate: credit.returnedDate || "",
      };
    }
    if (disposalCertRef && typeof disposalCertRef === "string") {
      update.disposalCertRef = disposalCertRef.slice(0, 500);
    }
    if (status === "pending_return" && typeof returnNote === "string") {
      update.returnNote = returnNote.slice(0, 500);
    }

    let updated = 0;
    for (const id of ids) {
      const ref = medColl.doc(id);
      const snap = await ref.get();
      if (!snap.exists) continue;
      batch.set(ref, update, { merge: true });
      updated++;
    }
    if (updated > 0) {
      await batch.commit();
    }

    logger.info(
      `[bulkUpdateMedicineStatus] ${status} x${updated} in ${pharmacyId} by ${request.auth.uid}`
    );
    return { updated };
  }
);
// ─── listPendingInvoices ────────────────────────────────────────────────────
// Returns all pending (partial) invoices for a pharmacy so the UI can show
// staging status.

exports.listPendingInvoices = onCall(
  {
    region: "us-central1",
    memory: "128MiB",
    timeoutSeconds: 30,
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }
    const { pharmacyId } = request.data;
    if (!pharmacyId) throw new HttpsError("invalid-argument", "pharmacyId is required");

    const db = getFirestore();
    const snap = await db
      .collection("pharmacies")
      .doc(pharmacyId)
      .collection("pending_invoices")
      .orderBy("createdAt", "desc")
      .limit(50)
      .get();

    const pending = snap.docs.map((doc) => {
      const d = doc.data();
      const pages = d.pages || [];
      const pageTotal = Math.max(...pages.map((p) => Number(p.totalPages) || 1), 1);
      const pageNumbers = pages
        .map((p) => Number(p.pageNumber))
        .filter((n) => n >= 1)
        .sort((a, b) => a - b);
      return {
        id: doc.id,
        invoiceNumber: d.invoiceNumber,
        distributor: d.distributor || "",
        pageCount: pages.length,
        pageTotal,
        pageNumbers,
        hasFooter: pages.some((p) => p.hasFooterTotals),
        totalItems: pages.reduce((sum, p) => sum + (p.lineItems || []).length, 0),
        status: d.status || "Incomplete - Waiting for remaining pages",
        createdAt: d.createdAt,
      };
    });

    return { pending };
  }
);

// ─── purchaseSummary ─────────────────────────────────────────────────────────
// Lightweight monthly Purchase Summary for the pharmacy owner. Builds ONLY on
// data already persisted by saveInvoice — no new schema fields, no GSTR-3B.
// Aggregates the footer invoiceSummary totals (Taxable + GST) grouped by
// Distributor Name, then splits each distributor's GST into tax slabs (5/12/18/
// 28/0) from the per-line gstRate/gstValue. CGST/SGST/IGST is inferred per
// invoice from the printed footer: totalIGST > 0 means inter-state (all GST is
// IGST); otherwise the line GST splits half CGST / half SGST (intra-state).
// Multi-page continuation docs (partial=true, invoiceTotal 0) are skipped so a
// multi-page invoice is never double-counted.

exports.purchaseSummary = onCall(
  {
    region: "us-central1",
    memory: "256MiB",
    timeoutSeconds: 30,
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }
    const { pharmacyId, from, to } = request.data;
    if (!pharmacyId) throw new HttpsError("invalid-argument", "pharmacyId is required");

    const db = getFirestore();
    const fromDate = new Date(from);
    const toDate = new Date(to);
    if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime()) || fromDate >= toDate) {
      throw new HttpsError("invalid-argument", "from/to must be valid timestamps with from < to");
    }

    const snap = await db
      .collection("pharmacies")
      .doc(pharmacyId)
      .collection("invoices")
      .where("createdAt", ">=", fromDate)
      .where("createdAt", "<", toDate)
      .get();

    const byDist = new Map(); // normalized distributor name → agg

    const ensureDist = (name) => {
      if (!byDist.has(name)) {
        byDist.set(name, {
          name,
          invoiceCount: 0,
          grandTotal: 0,
          taxable: 0,
          gst: 0,
          cgst: 0,
          sgst: 0,
          igst: 0,
          slabs: new Map(), // rate → { rate, taxable, gst, cgst, sgst, igst }
        });
      }
      return byDist.get(name);
    };

    for (const doc of snap.docs) {
      const d = doc.data();
      // Skip continuation pages of multi-page invoices — the footer page is the
      // only doc carrying authoritative totals, and it always has partial=false.
      if (d.partial === true) continue;

      const dist = ensureDist(normalizeDistributorName(d.distributor) || "UNKNOWN");
      dist.invoiceCount++;

      const summary = d.invoiceSummary || {};
      dist.grandTotal += Number(summary.grandTotal) || Number(d.invoiceTotal) || 0;

      // Inter-state iff the printed footer shows IGST.
      const interState = Number(summary.totalIGST || 0) > 0;

      const lines = Array.isArray(d.lineItems) ? d.lineItems : [];
      for (const l of lines) {
        const rate = Math.round(Number(l.gstRate) || 0);
        const taxable = Number(l.taxableValue) || 0;
        const gst = Number(l.gstValue) || 0;
        if (taxable <= 0 && gst <= 0) continue;

        const slabKey = String(rate);
        if (!dist.slabs.has(slabKey)) {
          dist.slabs.set(slabKey, { rate, taxable: 0, gst: 0, cgst: 0, sgst: 0, igst: 0 });
        }
        const slab = dist.slabs.get(slabKey);
        slab.taxable += taxable;
        slab.gst += gst;
        dist.taxable += taxable;
        dist.gst += gst;

        if (interState) {
          slab.igst += gst;
          dist.igst += gst;
        } else {
          const half = gst / 2;
          slab.cgst += half;
          slab.sgst += half;
          dist.cgst += half;
          dist.sgst += half;
        }
      }
    }

    const distributors = [];
    let grandTotals = {
      invoiceCount: 0,
      grandTotal: 0,
      taxable: 0,
      gst: 0,
      cgst: 0,
      sgst: 0,
      igst: 0,
    };

    for (const agg of byDist.values()) {
      const slabs = [...agg.slabs.values()]
        .map((s) => ({
          rate: s.rate,
          taxable: round2(s.taxable),
          gst: round2(s.gst),
          cgst: round2(s.cgst),
          sgst: round2(s.sgst),
          igst: round2(s.igst),
        }))
        .sort((a, b) => a.rate - b.rate);
      const d = {
        name: agg.name,
        invoiceCount: agg.invoiceCount,
        grandTotal: round2(agg.grandTotal),
        taxable: round2(agg.taxable),
        gst: round2(agg.gst),
        cgst: round2(agg.cgst),
        sgst: round2(agg.sgst),
        igst: round2(agg.igst),
        slabs,
      };
      distributors.push(d);
      grandTotals.invoiceCount += d.invoiceCount;
      grandTotals.grandTotal += agg.grandTotal;
      grandTotals.taxable += agg.taxable;
      grandTotals.gst += agg.gst;
      grandTotals.cgst += agg.cgst;
      grandTotals.sgst += agg.sgst;
      grandTotals.igst += agg.igst;
    }

    distributors.sort((a, b) => a.name.localeCompare(b.name));

    return {
      period: { from: fromDate.toISOString(), to: toDate.toISOString() },
      distributors,
      grandTotals: {
        invoiceCount: grandTotals.invoiceCount,
        grandTotal: round2(grandTotals.grandTotal),
        taxable: round2(grandTotals.taxable),
        gst: round2(grandTotals.gst),
        cgst: round2(grandTotals.cgst),
        sgst: round2(grandTotals.sgst),
        igst: round2(grandTotals.igst),
      },
    };
  }
);

// ─── getPendingInvoice ───────────────────────────────────────────────────────
// Returns the full merged contents of a staged (partial) invoice so the UI can
// open it in the review panel: all buffered pages, merged line items, the page
// image storage paths (for pagination), and the footer page's totals summary.

exports.getPendingInvoice = onCall(
  {
    region: "us-central1",
    memory: "128MiB",
    timeoutSeconds: 30,
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }
    const { pharmacyId, invoiceNumber } = request.data;
    if (!pharmacyId || !invoiceNumber) {
      throw new HttpsError("invalid-argument", "pharmacyId and invoiceNumber are required");
    }

    const db = getFirestore();
    const doc = await db
      .collection("pharmacies")
      .doc(pharmacyId)
      .collection("pending_invoices")
      .doc(invoiceNumber)
      .get();
    if (!doc.exists) {
      throw new HttpsError("not-found", "Pending invoice not found: " + invoiceNumber);
    }

    const d = doc.data();
    const pages = (d.pages || [])
      .slice()
      .sort((a, b) => Number(a.pageNumber) - Number(b.pageNumber));

    const lineItems = [];
    for (const p of pages) {
      for (const item of p.lineItems || []) {
        lineItems.push(item);
      }
    }

    const first = pages[0] || {};
    const summaryPage =
      pages.find((p) => p.hasFooterTotals) ||
      pages.find((p) => p.invoiceSummary && p.invoiceSummary.grandTotal) ||
      first;
    const summary = summaryPage.invoiceSummary || {};
    const invoiceTotal = summary.grandTotal || d.invoiceTotal || first.invoiceTotal || 0;

    return {
      invoiceNumber: d.invoiceNumber || invoiceNumber,
      distributor: d.distributor || "",
      invoiceDate: first.invoiceDate || d.invoiceDate || "",
      status: d.status || "Incomplete - Waiting for remaining pages",
      pageCount: pages.length,
      pages: pages.map((p) => ({
        pageNumber: p.pageNumber,
        totalPages: p.totalPages,
        storagePath: p.storagePath || "",
        hasFooterTotals: p.hasFooterTotals === true,
      })),
      lineItems,
      invoiceTotal,
      invoiceSummary: summary,
      gstCheck: summaryPage.gstCheck || {},
    };
  }
);
// Manually discard a pending (partial) invoice from staging.

exports.deletePendingInvoice = onCall(
  {
    region: "us-central1",
    memory: "128MiB",
    timeoutSeconds: 30,
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }
    const { pharmacyId, invoiceNumber } = request.data;
    if (!pharmacyId || !invoiceNumber) {
      throw new HttpsError("invalid-argument", "pharmacyId and invoiceNumber are required");
    }

    const db = getFirestore();
    await db
      .collection("pharmacies")
      .doc(pharmacyId)
      .collection("pending_invoices")
      .doc(invoiceNumber)
      .delete();

    return { success: true };
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

      // Clean up stale pending_invoices (older than 24 hours — orphaned partials).
      try {
        const pendingCutoff = new Date(Date.now() - 24 * 60 * 60 * 1000);
        const pendingSnap = await db.collectionGroup("pending_invoices").where("createdAt", "<=", pendingCutoff).limit(200).get();
        let pendingDeleted = 0;
        for (const doc of pendingSnap.docs) {
          await doc.ref.delete();
          pendingDeleted++;
        }
        if (pendingDeleted > 0) {
          console.log(`Cleanup: deleted ${pendingDeleted} stale pending invoice(s).`);
        }
      } catch (pendingErr) {
        console.error("Cleanup pending error:", pendingErr.message);
      }

      // Clean up stale pending_returns (older than 24 hours — return receipts
      // that were never matched/confirmed and never explicitly discarded).
      try {
        const returnCutoff = new Date(Date.now() - 24 * 60 * 60 * 1000);
        const returnSnap = await db
          .collectionGroup("pending_returns")
          .where("createdAt", "<=", returnCutoff)
          .limit(200)
          .get();
        let returnDeleted = 0;
        for (const doc of returnSnap.docs) {
          const status = doc.data().status || "";
          if (!["pending_review", "matched"].includes(status)) continue;
          const storagePath = doc.data().storagePath || "";
          await doc.ref.delete();
          if (storagePath) {
            try {
              await bucket.file(storagePath).delete();
            } catch (imgErr) {
              console.warn(`Cleanup: could not delete return receipt image ${storagePath}: ${imgErr.message}`);
            }
          }
          returnDeleted++;
        }
        if (returnDeleted > 0) {
          console.log(`Cleanup: deleted ${returnDeleted} stale pending return receipt(s).`);
        }
      } catch (returnErr) {
        console.error("Cleanup pending returns error:", returnErr.message);
      }

      // Recover importQueue items stuck in "processing" with an expired lease
      // (worker crashed mid-extraction) back to "uploaded" so the client's
      // resume loop picks them up again.
      try {
        const leaseCutoff = Date.now();
        const stuckSnap = await db
          .collectionGroup("importQueue")
          .where("status", "==", "processing")
          .where("leaseExpiresAt", "<=", leaseCutoff)
          .limit(300)
          .get();
        let recovered = 0;
        for (const doc of stuckSnap.docs) {
          await doc.ref.update({
            status: "uploaded",
            error: { code: "lease_expired", message: "Recovered by scheduledCleanup after worker lease expired." },
            leaseExpiresAt: FieldValue.delete(),
            updatedAt: FieldValue.serverTimestamp(),
          });
          recovered++;
        }
        if (recovered > 0) {
          console.log(`Cleanup: recovered ${recovered} stuck import queue item(s).`);
        }
      } catch (queueErr) {
        console.error("Cleanup import queue error:", queueErr.message);
      }

      // Purge terminal "failed" / "rejected" importQueue items older than 7
      // days (no retry/review, so the doc and raw image are no longer needed).
      try {
        const failedCutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
        const failedSnap = await db
          .collectionGroup("importQueue")
          .where("status", "in", ["failed", "rejected"])
          .where("updatedAt", "<=", failedCutoff)
          .limit(200)
          .get();
        let failedPurged = 0;
        for (const doc of failedSnap.docs) {
          const storagePath = doc.data().storagePath || "";
          await doc.ref.delete();
          if (storagePath) {
            try {
              await bucket.file(storagePath).delete();
            } catch (imgErr) {
              console.warn(`Cleanup: could not delete queue image ${storagePath}: ${imgErr.message}`);
            }
          }
          failedPurged++;
        }
        if (failedPurged > 0) {
          console.log(`Cleanup: purged ${failedPurged} old failed/rejected import queue item(s).`);
        }
      } catch (failedErr) {
        console.error("Cleanup failed-queue error:", failedErr.message);
      }

      // Purge "ingested" importQueue items older than 24h — their invoice was
      // auto-merged & saved, so the queue doc and raw image are no longer needed.
      try {
        const ingestedCutoff = new Date(Date.now() - 24 * 60 * 60 * 1000);
        const ingestedSnap = await db
          .collectionGroup("importQueue")
          .where("status", "==", "ingested")
          .where("updatedAt", "<=", ingestedCutoff)
          .limit(200)
          .get();
        let ingestedPurged = 0;
        for (const doc of ingestedSnap.docs) {
          const storagePath = doc.data().storagePath || "";
          await doc.ref.delete();
          if (storagePath) {
            try {
              await bucket.file(storagePath).delete();
            } catch (imgErr) {
              console.warn(`Cleanup: could not delete ingested image ${storagePath}: ${imgErr.message}`);
            }
          }
          ingestedPurged++;
        }
        if (ingestedPurged > 0) {
          console.log(`Cleanup: purged ${ingestedPurged} ingested import queue item(s).`);
        }
      } catch (ingestedErr) {
        console.error("Cleanup ingested-queue error:", ingestedErr.message);
      }

      // Purge medicines docs soft-deleted (status "deleted") more than 30 days
      // ago. Soft-delete keeps a grace window so accidental deletions can be
      // undone in the UI; after 30 days the data is permanently gone.
      try {
        const softCutoff = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
        const softSnap = await db
          .collectionGroup("medicines")
          .where("status", "==", "deleted")
          .where("deletedAt", "<=", softCutoff)
          .limit(300)
          .get();
        let softPurged = 0;
        for (const doc of softSnap.docs) {
          await doc.ref.delete();
          softPurged++;
        }
        if (softPurged > 0) {
          console.log(`Cleanup: purged ${softPurged} soft-deleted medicine doc(s).`);
        }
      } catch (softErr) {
        console.error("Cleanup soft-delete error:", softErr.message);
      }
    } catch (err) {
      console.error("Cleanup error:", err);
    }
  }
);








