/**
 * Multi-page invoice grouping.
 *
 * A group is keyed by GSTIN + invoiceNumber + invoiceDate (NOT distributor
 * name — casing varies and is unreliable). Each uploaded page goes through
 * Gemini extraction, then ingestExtractedPage appends it to the matching
 * invoiceGroups/{groupKeyHash} doc. A group is "complete" the moment ANY page
 * in it has hasFooterTotals = true; order of arrival never matters. Pages
 * missing GSTIN/invoiceNumber are routed to manualMatchQueue, never dropped.
 */
const crypto = require("crypto");
const admin = require("firebase-admin");

function buildGroupKey(gstin, invoiceNumber, invoiceDate) {
  // Normalize before hashing so casing/whitespace differences don't split groups.
  const normalized =
    `${(gstin || "UNKNOWN").trim().toUpperCase()}|` +
    `${(invoiceNumber || "UNKNOWN").trim().toUpperCase()}|` +
    `${(invoiceDate || "UNKNOWN").trim()}`;
  return crypto.createHash("sha256").update(normalized).digest("hex").slice(0, 24);
}

/**
 * Call this after a single image has been through Gemini extraction.
 * `extracted` is the JSON object matching the extraction schema (includes
 * gstin, invoiceNumber, invoiceDate, pageNumber, totalPages, hasFooterTotals,
 * looksLikeContinuationPage, footerData, lineItems).
 * `imageUrl` is the Storage path for this specific page.
 * `tenantId` is the multi-tenant scoping field (pharmacyId).
 */
async function ingestExtractedPage(extracted, imageUrl, tenantId) {
  const db = admin.firestore();

  // Guard: if GSTIN or invoiceNumber is missing entirely, this page can't be
  // grouped reliably. Don't silently drop it — route to a manual-match queue.
  if (!extracted.gstin || !extracted.invoiceNumber) {
    await db.collection("manualMatchQueue").add({
      imageUrl,
      extracted,
      tenantId,
      reason: "missing_gstin_or_invoice_number",
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    });
    return { routedToManualQueue: true };
  }

  const groupKey = buildGroupKey(extracted.gstin, extracted.invoiceNumber, extracted.invoiceDate);
  const groupRef = db.collection("invoiceGroups").doc(groupKey);

  const result = await db.runTransaction(async (tx) => {
    const doc = await tx.get(groupRef);

    const pageEntry = {
      imageUrl,
      pageNumber: extracted.pageNumber ?? null,
      totalPages: extracted.totalPages ?? null,
      hasFooterTotals: !!extracted.hasFooterTotals,
      looksLikeContinuationPage: !!extracted.looksLikeContinuationPage,
      lineItems: extracted.lineItems || [],
      footerData: extracted.footerData || null,
      uploadedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    let pages;
    if (!doc.exists) {
      pages = [pageEntry];
    } else {
      const existing = doc.data();
      // Idempotency: skip if this exact image was already ingested (retry-safe).
      const alreadyThere = existing.pages.some((p) => p.imageUrl === imageUrl);
      pages = alreadyThere ? existing.pages : [...existing.pages, pageEntry];
    }

    // Completeness: ANY page with a footer flips the whole group to complete.
    const footerPage = pages.find((p) => p.hasFooterTotals);
    const isComplete = !!footerPage;

    let mergedLineItems = null;
    let mergedFooterData = null;
    if (isComplete) {
      // Merge line items from all pages, sorted by pageNumber if present,
      // otherwise by upload order (stable sort keeps upload order for untagged pages).
      const sortedPages = [...pages].sort((a, b) => {
        if (a.pageNumber != null && b.pageNumber != null) return a.pageNumber - b.pageNumber;
        return 0;
      });
      mergedLineItems = sortedPages.flatMap((p) => p.lineItems);
      mergedFooterData = footerPage.footerData;
    }

    const payload = {
      gstin: extracted.gstin,
      invoiceNumber: extracted.invoiceNumber,
      invoiceDate: extracted.invoiceDate,
      tenantId,
      pages,
      status: isComplete ? "complete" : "incomplete",
      mergedLineItems,
      mergedFooterData,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      ...(doc.exists ? {} : { createdAt: admin.firestore.FieldValue.serverTimestamp() }),
    };

    tx.set(groupRef, payload, { merge: true });
    return { groupKey, isComplete, pageCount: pages.length };
  });

  return result;
}

module.exports = { ingestExtractedPage, buildGroupKey };
