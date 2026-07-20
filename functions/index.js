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
  "lineItems": [
    {
      "medicineName": "string — full medicine/product name",
      "batchNumber": "string — batch/lot number",
      "expiryDate": "string — expiry date in DD/MM/YYYY or MM/YYYY format",
      "quantityBilled": "number — quantity billed/purchased",
      "quantityFree": "number — free/complimentary pieces (0 if none)",
      "unitPrice": "number — price per unit before tax",
      "netValue": "number — taxable net value (qty × unitPrice)",
      "gstRate": "number — GST percentage (5, 12, 18, or 28)",
      "gstValue": "number — GST amount for this line",
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
- netValue should equal quantityBilled × unitPrice
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
            unitPrice: Math.max(0, parseFloat(item.unitPrice) || 0),
            netValue: Math.max(0, parseFloat(item.netValue) || 0),
            gstRate: [5, 12, 18, 28].includes(Number(item.gstRate)) ? Number(item.gstRate) : 12,
            gstValue: Math.max(0, parseFloat(item.gstValue) || 0),
            confidence: Math.max(0, Math.min(1, parseFloat(item.confidence) || 0.5))
        }));
    }

    // Recompute netValue and gstValue if needed
    result.lineItems.forEach(item => {
        if (item.unitPrice > 0 && item.quantityBilled > 0) {
            const expectedNet = +(item.unitPrice * item.quantityBilled).toFixed(2);
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
