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
 *   the raw file, runs the shared Gemini pipeline, and routes the result:
 *     * Partial (no footer totals / page N of M, N<M)  → pending_invoices
 *     * Full single-page invoice                        → status "extracted"
 *   Queue statuses: uploaded → processing → extracted/ingested → saved,
 *   with terminal states reviewed/saved/ingested/failed. Processing resumes on
 *   page load for items in "uploaded"/"extracted"/"processing" (lease expired).
 * - saveInvoice: Writes a confirmed invoice + medicine records. When queueId is
 *   supplied it guards against duplicate saves from stale client retries.
 * - ingestExtractedPage: Multi-page staging & auto-merge via structural
 *   pagination ("Page: X of Y").
 * - listPendingInvoices / deletePendingInvoice: staging management.
 * - scheduledCleanup: daily — deletes raw uploads older than 30 days, strips
 *   rawGeminiResponse from old invoices, drops stale pending invoices, recovers
 *   queue items stuck in "processing" with an expired lease, and purges "failed"
 *   queue items older than 7 days.
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
  const issues = [];
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
        issues.push(`"${l.medicineName || "?"}": gstValue ₹${gv.toFixed(2)} != taxable ₹${taxable.toFixed(2)} x ${rate}% = ₹${expected.toFixed(2)} (gstRate or gstValue misread)`);
      }
    }

    // Per-line net arithmetic: netValue should equal taxableValue + gstValue
    // This catches column swaps (taxable/net swapped) - bug #2 from VN-23-341141
    if (taxable > 0 && gv > 0 && net > 0) {
      const expectedNet = taxable + gv;
      if (Math.abs(net - expectedNet) > 1) {
        issues.push(`"${l.medicineName || "?"}": netValue ₹${net.toFixed(2)} != taxable ₹${taxable.toFixed(2)} + GST ₹${gv.toFixed(2)} = ₹${expectedNet.toFixed(2)} (columns misread)`);
      }
    }

    // Per-line taxableValue arithmetic: taxableValue ≈ unitPrice × qtyBilled × (1 - cdPercent/100)
    // This is the "GOLDEN ROW FORMULA" check from the prompt — bug #2 from VN-23-341141 (GUDCEF CV line)
    // On Vardhman/MCS invoices this often does NOT hold exactly due to additional per-line adjustments.
    // Use a wider tolerance (₹2) and only flag when the discrepancy is significant.
    if (unitPrice > 0 && qtyBilled > 0) {
      const expectedTaxable = unitPrice * qtyBilled * (1 - cdPercent / 100);
      if (taxable > 0 && Math.abs(taxable - expectedTaxable) > 2) {
        issues.push(`"${l.medicineName || "?"}": taxableValue ₹${taxable.toFixed(2)} != unitPrice ₹${unitPrice.toFixed(2)} × qty ${qtyBilled} × (1 - ${cdPercent}%) = ₹${expectedTaxable.toFixed(2)} (taxableValue or unitPrice/qty/CD% misread)`);
      }
    }
  }

  return issues;
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
7. SINGLE-PAGE INVOICE: If this page contains BOTH line items AND the footer totals, set hasFooterTotals = true, pageNumber = 1, totalPages = 1, looksLikeContinuationPage = false. If it is page 1 of a multi-page invoice (line items only, no footer), set pageNumber = 1, totalPages = 2 (or the printed Y), hasFooterTotals = false.
8. If multiple images are provided, they represent consecutive pages of the SAME invoice. Combine all line items. The footer totals are usually on the last page.
9. confidence values are 0.0 to 1.0 per field. If the image is blurry or unreadable, set captureQuality.readable = false and list reasons in issues[].
10. All amounts in INR as plain numbers (no ₹ symbol), typed as numbers, never strings. gstRate and cdPercent are percentages (e.g. 12, 4.00).
11. Return ONLY valid JSON — no markdown, no explanation.
12. SELF-CHECK before returning (this is mandatory):
    a. sum of ALL line gstValue values should approximately equal the printed Total GST (totalGst) — within ₹1.
    b. Verify the footer formula: saleValue − schDisc − cashDiscount + totalGst + roundOff − cnNo should approximately equal grandTotal (within ₹1). If it does not, you MISREAD a footer field — re-scan the footer summary block and CORRECT the specific misread field from the printed digits (pay special attention to the "Cash Disc." / "Sch Disc." rows — never read a printed discount as ₹0.00). Do NOT change line items to fake a match.
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

  // ── Partial-page detection ──────────────────────────────────────────────
  // A single page of a multi-page invoice (no footer totals, or Page X of Y
  // with X < Y, or a continuation page) CANNOT be validated against the
  // printed footer totals — they live on the last page. Never surface false
  // "GST self-check FAILED" mismatches or burn corrective Gemini turns on
  // incomplete data. All math validation is deferred until ingestExtractedPage
  // merges the full invoice.
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

  // pgNum > 1 OR pgTot > 1 always implies a multi-page invoice — even when
  // Gemini omitted totalPages and left a footer page at "1/1", the page index
  // alone proves staging is required (otherwise that page would bypass the
  // multi-page flow and be validated against only its own lines).
  const isPartialPage =
    pgNum > 1 ||
    pgTot > 1 ||
    !hasFooterTotals ||
    parsed.looksLikeContinuationPage === true;

  logger.info(
    `[gemini] page ${pgNum}/${pgTot} of ${parsed.invoiceNumber || "?"} — ${
      isPartialPage ? "PARTIAL (staged for multi-page merge)" : "SINGLE-PAGE (full invoice)"
    } (footer=${hasFooterTotals}, hints=${
      hints && Number(hints.pageNumber) >= 1 ? hints.pageNumber + "/" + hints.totalPages : "none"
    })`
  );

  let gstIssues = [];
  let gstAttempts = 0;
  if (isPartialPage) {
    logger.info(
      `[gemini] GST validation deferred for page ${pgNum}/${pgTot} until pages are merged.`
    );
  } else {
    // Deterministic GST enforcement: validate the arithmetic ourselves and, if it
    // fails, send Gemini a corrective turn telling it exactly which lines are
    // inconsistent. Gemini's in-prompt self-checks are advisory; this backstop is
    // what makes mixed-rate invoices (5/12/18 etc.) reliable.
    for (let attempt = 0; attempt <= 4; attempt++) {
      repairColumnSwaps(parsed);
      gstIssues = checkGstConsistency(parsed);
      gstAttempts = attempt + 1;
      if (gstIssues.length === 0) break;
      if (attempt === 4) break; // correction budget exhausted; final state already checked

      logger.warn(`[gemini] GST check failed (attempt ${attempt + 1}): ${gstIssues.join(" | ")}`);
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

  return {
    ...parsed,
    distributor: normalizeDistributorName(parsed.distributor || ""),
    hasFooterTotals,
    pageNumber: pgNum,
    totalPages: pgTot,
    looksLikeContinuationPage: parsed.looksLikeContinuationPage === true || pgNum > 1,
    gstCheck: isPartialPage
      ? { pass: true, deferred: true, attempts: 0, issues: [], message: "Validation deferred until all pages are merged." }
      : { pass: gstIssues.length === 0, attempts: gstAttempts, issues: gstIssues },
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

    try {
      // ── Queue guard (idempotency) ────────────────────────────────────────
      if (queueRef) {
        const qSnap = await queueRef.get();
        if (!qSnap.exists) {
          throw new HttpsError("not-found", "Queue item not found: " + queueId);
        }
        const qData = qSnap.data();
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
      const invoiceRef = await db.collection("pharmacies").doc(pharmacyId).collection("invoices").add({
        distributor: distName,
        invoiceNumber: safeInvoice.invoiceNumber || "",
        invoiceDate: safeInvoice.invoiceDate || "",
        invoiceTotal: safeInvoice.invoiceTotal || 0,
        invoiceSummary: safeInvoice.invoiceSummary || {},
        lineItems: safeLineItems,
        captureQuality: safeInvoice.captureQuality || {},
        gstCheck: safeInvoice.gstCheck || {},
        rawGeminiResponse: safeInvoice.rawGeminiResponse || "",
        pHash: safeInvoice.pHash || null,
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

      // Mark the queue item saved (idempotency committed AFTER the write).
      if (queueRef) {
        await queueRef.update({
          status: "saved",
          savedInvoiceId: invoiceRef.id,
          confirmedBy: confirmedBy || request.auth.uid,
          completedAt: FieldValue.serverTimestamp(),
          updatedAt: FieldValue.serverTimestamp(),
        });
      }

      logger.info(`[saveInvoice] Saved invoice ${invoiceRef.id} with ${safeLineItems.length} items for pharmacy=${pharmacyId}`);

      return { success: true, invoiceId: invoiceRef.id };
    } catch (err) {
      logger.error("[saveInvoice] Write failed:", err.message);
      throw new HttpsError("internal", "Failed to save: " + err.message);
    }
  }
);
// ─── ingestPageToPending (shared) ───────────────────────────────────────────
// Multi-page invoice staging & auto-merge via STRUCTURAL PAGINATION. Shared by
// ingestExtractedPage and processImportQueueItem.
//
// Flow:
// 1. Each page is extracted individually by runGeminiExtraction, which reports
//    pageNumber / totalPages (from the printed "Page: X of Y") and
//    hasFooterTotals.
// 2. This buffers every page into pending_invoices keyed by invoiceNumber.
// 3. It merges + saves to the invoices collection as soon as the invoice is
//    COMPLETE: either every page 1..totalPages is buffered, OR the page
//    carrying the final Grand Total footer summary has arrived (and the
//    invoice is not a known multi-pager with pages still missing).
// 4. Incomplete invoices stay staged with status
//    "Incomplete - Waiting for remaining pages" — they never fail/reject.
//
// Returns a discriminated result:
//   { status: "no-invoice-number", ... }
//   { status: "staged", pageNumber, pageTotal, pageCount, message }
//   { status: "merged", invoiceRef, invoiceNumber, footerPage, allLineItems,
//     effectiveTotal, gstIssues, message }
async function ingestPageToPending(db, { pharmacyId, extracted, storagePath, uid }) {
  const invoiceNumber = (extracted.invoiceNumber || "").trim();
  if (!invoiceNumber) {
    return {
      status: "no-invoice-number",
      message: "invoiceNumber is required for staging (continuation pages must repeat the invoice number from page 1).",
    };
  }

  // Pagination metadata — accept both pageNumber/page_current and
  // totalPages/page_total namings for forward compatibility.
  const pageCurrent = Number(extracted.pageNumber ?? extracted.page_current) || 1;
  const pageTotalIncoming = Number(extracted.totalPages ?? extracted.page_total) || 1;
  const hasFooter = extracted.hasFooterTotals === true;

  const pendingRef = db
    .collection("pharmacies")
    .doc(pharmacyId)
    .collection("pending_invoices")
    .doc(invoiceNumber);

  const pageData = {
    lineItems: sanitizeNumbers(extracted.lineItems || []),
    distributor: normalizeDistributorName(extracted.distributor || ""),
    invoiceDate: extracted.invoiceDate || "",
    pageNumber: pageCurrent,
    totalPages: pageTotalIncoming,
    hasFooterTotals: hasFooter,
    looksLikeContinuationPage: extracted.looksLikeContinuationPage || false,
    storagePath: storagePath || "",
    gstCheck: sanitizeNumbers(extracted.gstCheck || {}),
    invoiceSummary: sanitizeNumbers(extracted.invoiceSummary || {}),
    extracted: sanitizeNumbers(extracted),
    uploadedBy: uid,
    uploadedAt: new Date(),
  };

  // ── Buffer this page, then decide whether the invoice is complete ───────
  // Everything happens inside one transaction so concurrent page uploads for
  // the same invoice cannot both finalize / lose a page.
  let txResult;
  await db.runTransaction(async (tx) => {
    const doc = await tx.get(pendingRef);
    const base = doc.exists ? doc.data() : {};
    const existingPages = (base && base.pages) || [];

    // Dedupe: replace a re-uploaded page with the same number/path, but keep
    // all other pages so arrival order never matters.
    const deduped = [
      ...existingPages.filter(
        (p) => Number(p.pageNumber) !== pageCurrent && p.storagePath !== storagePath
      ),
      pageData,
    ].sort((a, b) => (Number(a.pageNumber) || 0) - (Number(b.pageNumber) || 0));

    // Effective total = highest totalPages any buffered page reports.
    const effectiveTotal = Math.max(
      pageTotalIncoming,
      ...deduped.map((p) => Number(p.totalPages) || 1)
    );

    // Completeness signals.
    const present = new Set(
      deduped.map((p) => Number(p.pageNumber)).filter((n) => n >= 1)
    );
    let allPagesPresent = true;
    for (let i = 1; i <= effectiveTotal; i++) {
      if (!present.has(i)) {
        allPagesPresent = false;
        break;
      }
    }
    const hasFooterAny = deduped.some((p) => p.hasFooterTotals);
    const isMultiPageClaim = effectiveTotal >= 2;

    // Complete when we possess every page 1..totalPages, OR when the Grand
    // Total footer page is present and the invoice is NOT a known multi-pager
    // still missing pages (avoids premature merge on out-of-order uploads).
    const complete = allPagesPresent || (hasFooterAny && !isMultiPageClaim);

    if (!complete) {
      tx.set(
        pendingRef,
        {
          invoiceNumber,
          distributor: normalizeDistributorName(extracted.distributor || base.distributor || ""),
          invoiceDate: extracted.invoiceDate || base.invoiceDate || "",
          tenantId: pharmacyId,
          pages: deduped,
          status: "Incomplete - Waiting for remaining pages",
          createdAt: base.createdAt || FieldValue.serverTimestamp(),
          updatedAt: FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      txResult = { complete: false, effectiveTotal, pageCount: deduped.length };
      return;
    }

    // ── Invoice complete — merge pages in page order and save ─────────────
    const footerPage =
      deduped.find((p) => p.hasFooterTotals) || deduped[deduped.length - 1];
    const allLineItems = deduped.flatMap((p) => p.lineItems || []);
    const summary = footerPage.invoiceSummary || extracted.invoiceSummary || {};
    const firstPage = deduped[0] || {};

    const merged = {
      distributor: normalizeDistributorName(footerPage.distributor || firstPage.distributor || ""),
      invoiceNumber,
      invoiceDate: footerPage.invoiceDate || firstPage.invoiceDate || "",
      invoiceTotal: summary.grandTotal || 0,
      invoiceSummary: summary,
      lineItems: allLineItems,
      captureQuality: footerPage.extracted?.captureQuality || {},
      rawGeminiResponse: footerPage.extracted?.rawGeminiResponse || "",
      hasFooterTotals: true,
      pageNumber: footerPage.pageNumber || effectiveTotal,
      totalPages: effectiveTotal,
      looksLikeContinuationPage: false,
      mergedFromPages: deduped.length,
    };

    // Deterministic GST validation on the merged record.
    const gstIssues = checkGstConsistency(merged);
    merged.gstCheck = { pass: gstIssues.length === 0, attempts: 1, issues: gstIssues };

    const invoiceRef = db
      .collection("pharmacies")
      .doc(pharmacyId)
      .collection("invoices")
      .doc();
    tx.set(invoiceRef, {
      distributor: merged.distributor,
      invoiceNumber: merged.invoiceNumber,
      invoiceDate: merged.invoiceDate,
      invoiceTotal: merged.invoiceTotal,
      invoiceSummary: merged.invoiceSummary,
      lineItems: merged.lineItems,
      captureQuality: merged.captureQuality,
      gstCheck: merged.gstCheck,
      rawGeminiResponse: merged.rawGeminiResponse,
      pHash: (footerPage.extracted && footerPage.extracted.pHash) || null,
      mergedFromPages: merged.mergedFromPages,
      confirmedBy: uid,
      confirmedAt: FieldValue.serverTimestamp(),
      createdAt: FieldValue.serverTimestamp(),
    });

    for (const item of allLineItems) {
      if (!item.medicineName) continue;
      const medRef = db
        .collection("pharmacies")
        .doc(pharmacyId)
        .collection("medicines")
        .doc();
      tx.set(medRef, {
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
        distributor: merged.distributor,
        invoiceId: invoiceRef.id,
        invoiceNumber: merged.invoiceNumber || "",
        distributorId: normalizeDistributorName(merged.distributor || ""),
        invoiceDate: merged.invoiceDate || "",
        pharmacyId,
        createdAt: FieldValue.serverTimestamp(),
        updatedAt: FieldValue.serverTimestamp(),
      });
    }

    tx.delete(pendingRef);

    txResult = {
      complete: true,
      effectiveTotal,
      invoiceRef,
      allLineItems,
      footerPage,
      gstIssues,
    };
  });

  if (!txResult.complete) {
    logger.info(
      `[ingestPageToPending] staged page ${pageCurrent}/${txResult.effectiveTotal} for invoice ${invoiceNumber} (${(extracted.lineItems || []).length} items, ${txResult.pageCount} buffered)`
    );
    return {
      status: "staged",
      invoiceNumber,
      pageNumber: pageCurrent,
      pageTotal: txResult.effectiveTotal,
      pageCount: txResult.pageCount,
      message: `Page ${pageCurrent} of ${txResult.effectiveTotal} saved. Incomplete - Waiting for remaining pages.`,
    };
  }

  logger.info(
    `[ingestPageToPending] merged & saved invoice ${invoiceNumber} → ${txResult.invoiceRef.id} (${txResult.allLineItems.length} items from ${txResult.effectiveTotal} pages)`
  );
  return {
    status: "merged",
    invoiceId: txResult.invoiceRef.id,
    invoiceNumber,
    pageNumber: txResult.footerPage.pageNumber || txResult.effectiveTotal,
    pageTotal: txResult.effectiveTotal,
    totalItems: txResult.allLineItems.length,
    totalPages: txResult.effectiveTotal,
    gstPass: txResult.gstIssues.length === 0,
    gstIssues: txResult.gstIssues,
    message: `Invoice merged from ${txResult.effectiveTotal} page(s) (${txResult.allLineItems.length} items) and saved.`,
  };
}

// ─── ingestExtractedPage ─────────────────────────────────────────────────────

exports.ingestExtractedPage = onCall(
  {
    region: "us-central1",
    memory: "256MiB",
    timeoutSeconds: 60,
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Login required.");
    }

    const { extracted, storagePath, pharmacyId } = request.data;
    if (!extracted) throw new HttpsError("invalid-argument", "extracted data is required");
    if (!pharmacyId) throw new HttpsError("invalid-argument", "pharmacyId is required");

    const db = getFirestore();
    const result = await ingestPageToPending(db, {
      pharmacyId,
      extracted,
      storagePath: storagePath || "",
      uid: request.auth.uid,
    });

    if (result.status === "no-invoice-number") {
      throw new HttpsError("invalid-argument", result.message);
    }
    return result;
  }
);
// ─── processImportQueueItem ───────────────────────────────────────────────────
// The resilient bulk-import worker.
//
// Queue contract (documented in the frontend too):
//   status: "uploaded" (raw file persisted, nothing else) →
//           "processing" (leased claim by a worker) →
//           "extracted" (full single-page invoice, raw result stored for
//                        review) | "ingested" (multi-page invoice merged &
//                        saved) | "ingested-partial" (page staged) →
//           "saved" (user confirmed via saveInvoice)
//   Terminal states: "saved", "reviewed", "ingested", "failed", "rejected".
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
    const isPartial =
      extracted.pageNumber > 1 ||
      extracted.totalPages > 1 ||
      !extracted.hasFooterTotals ||
      extracted.looksLikeContinuationPage === true;

    if (isPartial) {
      // Page of a multi-page invoice → stage in pending_invoices; the merge is
      // handled by ingestPageToPending when the final page arrives.
      const ingestResult = await ingestPageToPending(db, {
        pharmacyId,
        extracted,
        storagePath,
        uid: request.auth.uid,
      });
      if (ingestResult.status === "no-invoice-number") {
        return await markFailed("missing_invoice_number", ingestResult.message);
      }
      const queueStatus = ingestResult.status === "merged" ? "ingested" : "ingested-partial";
      await queueRef.update({
        status: queueStatus,
        extracted,
        ingestResult,
        completedAt: FieldValue.serverTimestamp(),
        leaseExpiresAt: FieldValue.delete(),
        updatedAt: FieldValue.serverTimestamp(),
      });
      logger.info(`[processImportQueueItem] ${imageId} → ${queueStatus} (invoice ${ingestResult.invoiceNumber || "?"})`);
      return { status: queueStatus, ...ingestResult };
    }

    // Full single-page invoice → persist raw result for review; the client
    // (or a future review screen) calls saveInvoice with queueId to confirm.
    await queueRef.update({
      status: "extracted",
      extracted,
      invoiceNumber: extracted.invoiceNumber || "",
      gstCheck: extracted.gstCheck || {},
      completedAt: FieldValue.serverTimestamp(),
      leaseExpiresAt: FieldValue.delete(),
      updatedAt: FieldValue.serverTimestamp(),
    });
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

      // Purge "failed" importQueue items older than 7 days (terminal, no retry).
      try {
        const failedCutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
        const failedSnap = await db
          .collectionGroup("importQueue")
          .where("status", "==", "failed")
          .where("updatedAt", "<=", failedCutoff)
          .limit(200)
          .get();
        let failedPurged = 0;
        for (const doc of failedSnap.docs) {
          await doc.ref.delete();
          failedPurged++;
        }
        if (failedPurged > 0) {
          console.log(`Cleanup: purged ${failedPurged} old failed import queue item(s).`);
        }
      } catch (failedErr) {
        console.error("Cleanup failed-queue error:", failedErr.message);
      }
    } catch (err) {
      console.error("Cleanup error:", err);
    }
  }
);







