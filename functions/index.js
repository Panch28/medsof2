/**
 * RxExpiry Cloud Functions
 * - extractInvoice: Synchronous HTTPS callable. Accepts a Storage path,
 *   downloads the file, sends to Gemini gemini-2.0-flash, returns structured data.
 * - scheduledCleanup: Runs daily, deletes raw uploads older than 30 days.
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { defineSecret } = require("firebase-functions/params");
const { initializeApp } = require("firebase-admin/app");
const { getStorage } = require("firebase-admin/storage");
const { getFirestore, FieldValue } = require("firebase-admin/firestore");
const { GoogleGenerativeAI } = require("@google/generative-ai");

initializeApp();

const GEMINI_API_KEY = defineSecret("GEMINI_API_KEY");

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
    "cashDiscount": number,
    "totalGst": number,
    "roundOff": number,
    "grandTotal": number
  },
  "lineItems": [
    {
      "medicineName": "string",
      "batchNumber": "string",
      "expiryDate": "string (MM/YYYY or as printed)",
      "quantityBilled": number,
      "quantityFree": number,
      "unitPrice": number,
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
        "netValue": number,
        "gstRate": number,
        "gstValue": number
      }
    }
  ]
}

CRITICAL CALCULATION RULES:
1. DO NOT independently sum row-level netValues to find the final Grand Total if an invoice-level Cash Discount exists. ALWAYS use the printed summary block.
2. Row-level netValue = Taxable Value + row gstValue. Invoice-level Cash Discount is deducted from the total taxable amount BEFORE GST — it does NOT appear in individual line items.
3. Official summary formula: saleValue - cashDiscount + totalGst + roundOff = grandTotal. Extract these from the invoice footer — do not compute them from line items.
4. If multiple images are provided, they represent consecutive pages of the SAME invoice. Combine all line items. The grandTotal is usually on the last page.

ROW-LEVEL EXTRACTION:
- Use these exact column mappings: RACK | DESCRIPTION (medicineName) | Billed Qty (quantityBilled) | Free Qty (quantityFree) | Pack | Batch No. (batchNumber) | Exp. Date (expiryDate) | MRP | Trade Price (unitPrice) | CD % | Taxable Value | GST % (gstRate) | Net Value (netValue) | Mfr/Mkt | HSN Code.
- If a field is missing, output default values (0 or "") — never shift cells out of alignment.
- quantityFree is the free/bonus column (often "Free" or "Scheme Qty") — use 0 if absent.

Rules:
- confidence values are 0.0 to 1.0 per field.
- If the image is blurry or unreadable, set captureQuality.readable = false and list reasons in issues[].
- gstRate is the percentage (e.g. 12 for 12%), gstValue is the rupee amount.
- All amounts in INR as plain numbers (no ₹ symbol).
- Return ONLY valid JSON — no markdown, no explanation.`;

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

    return parsed;
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
      console.log(`Cleanup complete. Deleted ${deleted} stale file(s).`);
    } catch (err) {
      console.error("Cleanup error:", err);
    }
  }
);
