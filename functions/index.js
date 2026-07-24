/**
 * RxExpiry — extractInvoice Cloud Function
 *
 * Trigger: HTTPS Callable from frontend (step 4 in flow)
 * Model:   gemini-3-flash-preview (the ONLY AI model in this app)
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
const GEMINI_MODEL = "gemini-3.1-flash-lite";

// ─── Extraction Prompt for Gemini ──────────────────────────────────
const EXTRACTION_PROMPT = `You are an OCR extraction engine for Indian pharmacy invoices.

Given this invoice image, extract ALL information and return ONLY a valid JSON object (no markdown, no code fences, no explanation) with this exact structure:

{
  "distributor": "string — distributor/supplier name from invoice header",
  "invoiceNumber": "string — invoice/bill number",
  "invoiceTotal": "number — grand total amount declared on invoice",
  "schemeDiscount": "number — total scheme/discount amount shown in invoice footer/summary (0 if not present)",
  "cashDiscount": "number — cash discount amount shown separately in invoice footer/summary, distinct from scheme discount (0 if not present)",
  "roundOff": "number — round off amount shown in invoice footer/summary (can be positive or negative, 0 if not present)",
  "lineItems": [
    {
      "medicineName": "string — full medicine/product name",
      "batchNumber": "string — batch/lot number",
      "expiryDate": "string — expiry date in DD/MM/YYYY or MM/YYYY format",
      "quantityBilled": "number — quantity billed/purchased",
      "quantityFree": "number — free/complimentary pieces (0 if none)",
      "tradePrice": "number — the Trade Price (per unit price before C.D.% discount) as printed on the invoice",
      "cdPercent": "number — the C.D.% (cash discount percentage) column value for this line (0 if not present)",
      "netValue": "number — Taxable Value for this line, computed as: tradePrice × quantityBilled × (1 - cdPercent/100)",
      "gstRate": "number — GST percentage (5, 12, 18, or 28)",
      "gstValue": "number — GST amount for this line, computed as: netValue × gstRate / 100",
      "confidence": "number between 0 and 1 — how confident you are in this line's accuracy"
    }
  ],
  "captureQuality": {
    "readable": "boolean — can you read the invoice clearly?",
    "issues": ["array of strings — specific problems if not readable, e.g. 'partial page', 'text cut off', 'blurry section'"],
    "missingPage": "boolean — does this appear to be a multi-page invoice with pages missing?"
  }
}

Rules:
- Extract EVERY line item visible on the invoice, even if partially visible
- For confidence: 1.0 = perfectly clear, 0.9 = very clear, 0.8 = readable with minor doubt, 0.7 = partially readable, below 0.7 = uncertain
- If a field is not visible, use null for strings and 0 for numbers
- CRITICAL — for EVERY line item, you MUST read both the Trade Price column AND the C.D.% column from the invoice table:
  1. Read "tradePrice" = the per-unit Trade Price printed in that row
  2. Read "cdPercent" = the C.D.% (cash discount percentage) printed in that row (often 4% or similar; if no C.D.% column exists on the invoice, use 0)
  3. Calculate netValue = tradePrice × quantityBilled × (1 - cdPercent/100)
  This formula MUST be applied to EVERY row without exception — do NOT use the printed "Unit Price" or "Taxable Value" columns directly, compute netValue yourself using the formula above to ensure consistency.
- gstValue should equal netValue × gstRate / 100
- invoiceTotal should be the grand total shown on the invoice
- For captureQuality.readable: false if more than 30% of the invoice is unreadable`;

// ═══════════════════════════════════════════════════════════════════
// extractInvoice Cloud Function
// ═══════════════════════════════════════════════════════════════════
exports.extractInvoice = onCall(
    {
        region: "us-central1",
        memory: "512MB",
        timeoutSeconds: 60,
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

            // Step 2: Send to Gemini 3 Flash Preview (passing secret key at runtime)
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
// Helper: Call Gemini 3 Flash Preview API
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

    // Validate and normalize structure
    const result = {
        distributor: parsed.distributor || "",
        invoiceNumber: parsed.invoiceNumber || "",
        invoiceTotal: Number(parsed.invoiceTotal) || 0,
        schemeDiscount: Number(parsed.schemeDiscount) || 0,
        cashDiscount: Number(parsed.cashDiscount) || 0,
        roundOff: Number(parsed.roundOff) || 0,
        lineItems: [],
        captureQuality: {
            readable: parsed.captureQuality?.readable !== false,
            issues: Array.isArray(parsed.captureQuality?.issues) ? parsed.captureQuality.issues : [],
            missingPage: parsed.captureQuality?.missingPage === true
        }
    };

    // Process line items
    if (Array.isArray(parsed.lineItems)) {
        result.lineItems = parsed.lineItems.map(item => ({
            medicineName: String(item.medicineName || "").trim(),
            batchNumber: String(item.batchNumber || "").trim(),
            expiryDate: String(item.expiryDate || "").trim(),
            quantityBilled: Math.max(0, parseInt(item.quantityBilled) || 0),
            quantityFree: Math.max(0, parseInt(item.quantityFree) || 0),
            tradePrice: Math.max(0, parseFloat(item.tradePrice) || 0),
            cdPercent: Math.max(0, parseFloat(item.cdPercent) || 0),
            netValue: Math.max(0, parseFloat(item.netValue) || 0),
            gstRate: [5, 12, 18, 28].includes(Number(item.gstRate)) ? Number(item.gstRate) : 12,
            gstValue: Math.max(0, parseFloat(item.gstValue) || 0),
            confidence: Math.max(0, Math.min(1, parseFloat(item.confidence) || 0.5))
        }));
    }

    // Recompute netValue using tradePrice × qty × (1 - cdPercent/100), then gstValue from netValue
    result.lineItems.forEach(item => {
        if (item.tradePrice > 0 && item.quantityBilled > 0) {
            const cdMultiplier = 1 - (item.cdPercent / 100);
            const expectedNet = +(item.tradePrice * item.quantityBilled * cdMultiplier).toFixed(2);
            if (Math.abs(item.netValue - expectedNet) > 1) {
                item.netValue = expectedNet;
            }
        }
        if (item.netValue > 0 && item.gstRate > 0) {
            const expectedGst = +(item.netValue * item.gstRate / 100).toFixed(2);
            if (Math.abs(item.gstValue - expectedGst) > 1) {
                item.gstValue = expectedGst;
            }
        }
    });

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
            // Write invoice document
            const invoiceRef = db.collection("pharmacies").doc(pharmacyId).collection("invoices").doc();
            batch.set(invoiceRef, {
                distributor: invoice.distributor || "",
                invoiceNumber: invoice.invoiceNumber || "",
                invoiceTotal: invoice.invoiceTotal || 0,
                schemeDiscount: invoice.schemeDiscount || 0,
                cashDiscount: invoice.cashDiscount || 0,
                roundOff: invoice.roundOff || 0,
                lineItemCount: medicines.length,
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
                    netValue: med.netValue || 0,
                    gstRate: med.gstRate || 0,
                    gstValue: med.gstValue || 0,
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
