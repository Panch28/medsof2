/**
 * RxExpiry - Full production script
 *
 * Flow:
 * 1. Phone OTP auth (Firebase Auth)
 * 2. File selection (camera/gallery/PDF) — single or batch
 * 3. Each raw file is uploaded to Storage and immediately recorded in the
 *    persisted import queue at /pharmacies/{id}/importQueue/{imageId} with
 *    status "uploaded" BEFORE any AI work (browser close is now safe).
 * 4. processImportQueueItem CF extracts each item (leased, resumable, Gemini
 *    only — no local OCR). Multi-page pages are staged & merged server-side.
 * 5. Full invoices open the Review screen: image + editable fields, confidence
 *    highlights, arithmetic check → Confirm & Save → saveInvoice CF writes
 *    Firestore medicines + invoice docs and marks the queue item "saved".
 * 6. On page load, resumeImportQueue() re-processes any item whose status is
 *    not yet terminal (uploaded / processing / extracted / ingested-partial).
 */

import { firebaseConfig } from "./firebaseConfig.js";
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import {
  getAuth,
  RecaptchaVerifier,
  signInWithPhoneNumber,
  onAuthStateChanged,
  signOut,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";
import {
  getFirestore,
  collection,
  getDocs,
  doc,
  getDoc,
  setDoc,
  updateDoc,
  query,
  where,
  onSnapshot,
  orderBy,
  limit,
  serverTimestamp,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";
import {
  getStorage,
  ref,
  uploadBytes,
  deleteObject,
  getDownloadURL,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-storage.js";
import {
  getFunctions,
  httpsCallable,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-functions.js";




const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);
const storage = getStorage(app);
const functions = getFunctions(app, "us-central1");

// Global error capture — logs any runtime error to the console so failures
// (e.g. during save) can be diagnosed.
window.addEventListener("error", (e) => console.error("[Global error]", e.message, e.error && e.error.stack));
window.addEventListener("unhandledrejection", (e) => console.error("[Unhandled rejection]", e.reason && e.reason.stack ? e.reason.stack : e.reason));

// Configure PDF.js worker Src
if (window.pdfjsLib) {
  window.pdfjsLib.GlobalWorkerOptions.workerSrc = "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.4.120/pdf.worker.min.js";
}

// ─── State ────────────────────────────────────────────────────────────────────

let currentPharmacyId = "city-pharma";
let currentUser = null;
let confirmationResult = null;
let cameraStream = null;

// Holds data for the active review session
let reviewSession = {
  storagePaths: [],
  objectUrls: [],
  currentPageIndex: 0,
  fileType: 'image',
  extracted: null,
  queueId: null,
  pendingInvoiceNumber: null,
};

// Import queue state (persisted queue drives bulk import; see
// "Import Queue (persisted, resumable)" section below).
let importQueue = [];
let importQueueCursor = 0;
let importQueueBusy = false;
// True only while a live upload batch is running (advanceImportQueue keeps
// chaining review modals). On a resume-from-disk load this stays false so the
// app lands on Home with the queue list and never auto-opens review modals.
let importQueueAuto = false;
let reviewChainActive = false; // true while the one-by-one review chain is open

// ─── DOM Helpers ──────────────────────────────────────────────────────────────

const $ = (id) => document.getElementById(id);

function showToast(msg, type = "info") {
  const toast = $("toast");
  const txt = $("toast-text");
  const ico = $("toast-icon");
  if (!toast) return;
  txt.textContent = msg;
  const colors = { success: "bg-emerald-500", error: "bg-rose-500", warning: "bg-amber-500", info: "bg-indigo-500" };
  ico.className = "w-2 h-2 rounded-full " + (colors[type] || colors.info);
  toast.classList.remove("translate-y-[-100px]", "opacity-0");
  toast.classList.add("translate-y-0", "opacity-100");
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => {
    toast.classList.add("translate-y-[-100px]", "opacity-0");
    toast.classList.remove("translate-y-0", "opacity-100");
  }, 3000);
}

function showScreen(id) {
  ["auth-screen", "app-workspace"].forEach((s) => {
    const el = $(s);
    if (el) el.classList.add("hidden");
  });
  const target = $(id);
  if (target) target.classList.remove("hidden");
}

// ─── Auth ─────────────────────────────────────────────────────────────────────

function initAuth() {
  // Pharmacy selector
  const pharmacySelect = $("auth-pharmacy-id");
  if (pharmacySelect) {
    pharmacySelect.addEventListener("change", () => {
      currentPharmacyId = pharmacySelect.value;
      if (currentPharmacyId === "new-pharmacy") {
        $("new-pharmacy-form").classList.remove("hidden");
      } else {
        $("new-pharmacy-form").classList.add("hidden");
      }
    });
  }

  // Role switcher
  document.querySelectorAll(".role-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".role-btn").forEach((b) => {
        b.classList.remove("bg-indigo-600", "text-white", "shadow-sm");
        b.classList.add("text-slate-400");
      });
      btn.classList.add("bg-indigo-600", "text-white", "shadow-sm");
      btn.classList.remove("text-slate-400");
    });
  });

  // reCAPTCHA (invisible)
  window.recaptchaVerifier = new RecaptchaVerifier(auth, "recaptcha-container", {
    size: "invisible",
    callback: () => {},
  });

  const submitBtn = $("auth-submit-btn");
  const phoneInput = $("auth-phone");
  const otpContainer = $("otp-container");
  const otpInput = $("auth-otp");

  submitBtn.addEventListener("click", async () => {
    if (!confirmationResult) {
      // Step 1: Send OTP
      const phone = "+91" + phoneInput.value.replace(/\s/g, "");
      if (phone.length !== 13) {
        showToast("Enter a valid 10-digit phone number", "error");
        return;
      }
      submitBtn.textContent = "Sending...";
      submitBtn.disabled = true;
      try {
        confirmationResult = await signInWithPhoneNumber(auth, phone, window.recaptchaVerifier);
        otpContainer.classList.remove("hidden");
        phoneInput.disabled = true;
        submitBtn.textContent = "Verify OTP";
        submitBtn.disabled = false;
        showToast("OTP sent!", "success");
      } catch (err) {
        showToast("Error: " + err.message, "error");
        submitBtn.textContent = "Send OTP";
        submitBtn.disabled = false;
        confirmationResult = null;
      }
    } else {
      // Step 2: Verify OTP
      const code = otpInput.value.trim();
      if (code.length !== 6) {
        showToast("Enter the 6-digit OTP", "error");
        return;
      }
      submitBtn.textContent = "Verifying...";
      submitBtn.disabled = true;
      try {
        await confirmationResult.confirm(code);
        // onAuthStateChanged handles the rest
      } catch (err) {
        showToast("Invalid OTP. Try again.", "error");
        submitBtn.textContent = "Verify OTP";
        submitBtn.disabled = false;
      }
    }
  });

  $("auth-logout-btn").addEventListener("click", async () => {
    await signOut(auth);
    confirmationResult = null;
    showScreen("auth-screen");
    showToast("Logged out", "info");
  });

  onAuthStateChanged(auth, (user) => {
    if (user) {
      currentUser = user;
      showScreen("app-workspace");
      $("header-user-status").textContent = currentPharmacyId;
      initApp();
    } else {
      currentUser = null;
      showScreen("auth-screen");
    }
  });
}

// ─── App Init (post-login) ────────────────────────────────────────────────────

function initApp() {
  initNavTabs();
  initThemeToggle();
  initUploadHandlers();
  subscribeExpiringMedicines();
  subscribeDistributors();
  subscribePendingInvoices();
  initPurchaseReports();
  resumeImportQueue();
}

// ─── Nav Tabs ─────────────────────────────────────────────────────────────────

function initNavTabs() {
  const tabs = document.querySelectorAll(".nav-tab");
  const panes = document.querySelectorAll(".view-pane");

  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      tabs.forEach((t) => {
        t.classList.remove("text-indigo-500");
        t.classList.add("text-slate-500");
      });
      panes.forEach((p) => p.classList.add("hidden"));
      tab.classList.remove("text-slate-500");
      tab.classList.add("text-indigo-500");
      const pane = $(tab.dataset.target);
      if (pane) pane.classList.remove("hidden");
    });
  });
}

// ─── Theme Toggle ─────────────────────────────────────────────────────────────

function initThemeToggle() {
  $("theme-toggle")?.addEventListener("click", () => {
    document.documentElement.classList.toggle("dark");
  });
}

// ─── Upload Handlers ──────────────────────────────────────────────────────────

function initUploadHandlers() {
  const btnCameraScan = $("btn-camera-scan");
  const cameraContainer = $("camera-feed-container");
  const btnCloseCamera = $("btn-close-camera");
  const btnCapture = $("btn-camera-capture");
  const videoEl = $("camera-stream");

  // Camera open
  btnCameraScan?.addEventListener("click", async () => {
    try {
      cameraStream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: "environment", width: { ideal: 1920 }, height: { ideal: 1080 } },
      });
      videoEl.srcObject = cameraStream;
      cameraContainer.classList.remove("hidden");
    } catch (err) {
      showToast("Camera access denied: " + err.message, "error");
    }
  });

  // Camera close
  btnCloseCamera?.addEventListener("click", () => {
    stopCamera();
    cameraContainer.classList.add("hidden");
  });

  // Camera capture
  btnCapture?.addEventListener("click", () => {
    const canvas = document.createElement("canvas");
    canvas.width = videoEl.videoWidth;
    canvas.height = videoEl.videoHeight;
    canvas.getContext("2d").drawImage(videoEl, 0, 0);
    stopCamera();
    cameraContainer.classList.add("hidden");
    canvas.toBlob(async (blob) => {
      const file = new File([blob], `capture_${Date.now()}.jpg`, { type: "image/jpeg" });
      await handleFileSelected(file);
    }, "image/jpeg", 0.92);
  });

  // Gallery upload
  $("upload-gallery")?.addEventListener("change", async (e) => {
    const files = Array.from(e.target.files);
    if (files.length > 0) {
      await handleMultipleFilesSelected(files);
      e.target.value = ""; // reset
    }
  });

  // PDF upload
  $("upload-pdf")?.addEventListener("change", async (e) => {
    const file = e.target.files[0];
    if (file) {
      showLoadingOverlay(true, "Splitting PDF into page images...");
      try {
        const pageFiles = await splitPdfToImages(file);
        showLoadingOverlay(false);
        await handleMultipleFilesSelected(pageFiles);
      } catch (err) {
        showLoadingOverlay(false);
        showToast("Failed to split PDF: " + err.message, "error");
      }
      e.target.value = "";
    }
  });

  // Mobile pane toggle in review modal
  $("btn-toggle-to-form")?.addEventListener("click", () => {
    $("review-visual-container").classList.add("hidden");
    $("review-form-container").classList.remove("hidden", "md:flex");
    $("review-form-container").classList.add("flex");
  });
  $("btn-toggle-to-scan")?.addEventListener("click", () => {
    $("review-visual-container").classList.remove("hidden");
    $("review-form-container").classList.add("hidden");
    $("review-form-container").classList.remove("flex");
  });

  // Review pagination controls
  $("btn-prev-page")?.addEventListener("click", () => {
    if (reviewSession.currentPageIndex > 0) {
      reviewSession.currentPageIndex--;
      updateReviewVisualSource();
      updateReviewPaginationUI();
    }
  });
  $("btn-next-page")?.addEventListener("click", () => {
    if (reviewSession.currentPageIndex < reviewSession.objectUrls.length - 1) {
      reviewSession.currentPageIndex++;
      updateReviewVisualSource();
      updateReviewPaginationUI();
    }
  });
}

function stopCamera() {
  if (cameraStream) {
    cameraStream.getTracks().forEach((t) => t.stop());
    cameraStream = null;
  }
}

// ─── Core Pipeline ────────────────────────────────────────────────────────────

async function handleFileSelected(file) {
  // Single-file path (camera) — routes through the same persisted queue as bulk.
  console.log("[handleFileSelected] Starting upload for", file.name, file.type, file.size);
  showLoadingOverlay(true, "Uploading...");
  let item;
  try {
    item = await enqueueFile(file);
    console.log("[handleFileSelected] Upload succeeded, queue item:", item);
  } catch (err) {
    console.error("[handleFileSelected] Upload FAILED:", err.code, err.message, err);
    showLoadingOverlay(false);
    showToast("Upload failed: " + err.message, "error");
    return;
  }
  showLoadingOverlay(false);
  importQueue.push(item);
  renderImportQueueStatus();
  startImportQueue();
}

// ─── Loading Overlay ──────────────────────────────────────────────────────────

function showLoadingOverlay(show, message = "Processing...") {
  let overlay = $("loading-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "loading-overlay";
    overlay.className = "fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-[100] flex flex-col items-center justify-center gap-4 cursor-pointer";
    overlay.innerHTML = `
      <div class="w-16 h-16 rounded-2xl bg-indigo-600/20 border border-indigo-500/30 flex items-center justify-center">
        <svg class="w-8 h-8 text-indigo-400 animate-spin" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
        </svg>
      </div>
      <p id="loading-msg" class="text-sm font-semibold text-slate-300"></p>
      <button id="loading-dismiss" type="button" class="px-3 py-1.5 rounded-lg bg-slate-800/80 border border-slate-700 text-slate-300 text-[10px] font-bold hover:bg-slate-700 hover:text-white transition-all">✕ Continue in background</button>
    `;
    // Never trap the owner on a blocking screen: clicking the backdrop or the
    // button hides it. The underlying job (upload / extraction) keeps running —
    // the import queue is persisted in Firestore, so a refresh or navigation is
    // always safe and the resume flow re-extracts any leftovers.
    overlay.addEventListener("click", () => overlay.classList.add("hidden"));
    document.body.appendChild(overlay);
  }
  if (show) {
    $("loading-msg").textContent = message;
    overlay.classList.remove("hidden");
  } else {
    overlay.classList.add("hidden");
  }
}

// ─── Review Panel ─────────────────────────────────────────────────────────────

function openReviewPanel(extracted) {
  const panel = $("extraction-review-panel");
  panel.classList.remove("hidden");
  panel.style.display = "flex"; // defensive: no CSS specificity/order surprise can keep it hidden

  // Show image or PDF with pagination
  const pagEl = $("review-pagination");
  if (reviewSession.objectUrls && reviewSession.objectUrls.length > 1) {
    pagEl.classList.remove("hidden");
    updateReviewPaginationUI();
  } else {
    pagEl.classList.add("hidden");
  }
  updateReviewVisualSource();

  // Header info
  $("review-distributor-lbl").textContent = `${extracted.distributor || "Unknown Distributor"} · Invoice #${extracted.invoiceNumber || "—"}`;

  // Invoice summary (cash discount, round-off, etc.)
  const summary = extracted.invoiceSummary;
  window._invoiceSummary = summary || null;
  // Printed-footer assertions (printed GST, grand-total formula) are only valid
  // when THIS page IS the complete invoice: it carries the footer totals AND no
  // other pages exist. On a continuation page — or the footer page of a
  // multi-page invoice, where the printed totals cover the WHOLE invoice over
  // only a subset of the lines — comparing the printed footer against this
  // page's line sum fires a permanent false "GST mismatch" (see multi-page
  // invoices). Per-line arithmetic checks still run on every page below.
  const printedContMarker = /continue|next page|cont\.?/i.test(extracted.printedPagination || "");
  window._footerChecksValid =
    extracted.hasFooterTotals === true &&
    !(extracted.looksLikeContinuationPage === true) &&
    !printedContMarker &&
    (Number(extracted.totalPages) || 1) <= 1;
  console.log(
    "[review] footer gate:", {
      hasFooterTotals: extracted.hasFooterTotals,
      looksLikeContinuationPage: extracted.looksLikeContinuationPage,
      totalPages: extracted.totalPages,
      pageNumber: extracted.pageNumber,
      printedPagination: extracted.printedPagination,
      printedContMarker,
      _footerChecksValid: window._footerChecksValid,
      grandTotal: summary?.grandTotal ?? summary?.invoiceTotal,
      gstCheck: extracted.gstCheck,
    }
  );
  const cashDiscRow = $("review-cash-disc-row");
  const schDiscRow = $("review-sch-disc-row");
  const cnNoRow = $("review-cn-no-row");
  const roundOffRow = $("review-round-off-row");
  const printedGstRow = $("review-printed-gst-row");
  const printedCdRow = $("review-printed-cd-row");
  if (summary && window._footerChecksValid) {
    $("review-declared-total-input").value = (summary.grandTotal || 0).toFixed(2);
    cashDiscRow.classList.remove("hidden");
    $("review-cash-disc-val").textContent = "-₹" + (summary.cashDiscount || 0).toFixed(2);
    if (schDiscRow) {
      if (summary.schDisc) {
        schDiscRow.classList.remove("hidden");
        $("review-sch-disc-val").textContent = "-₹" + summary.schDisc.toFixed(2);
      } else {
        schDiscRow.classList.add("hidden");
      }
    }
    if (cnNoRow) {
      if (summary.cnNo) {
        cnNoRow.classList.remove("hidden");
        $("review-cn-no-val").textContent = "-₹" + summary.cnNo.toFixed(2);
      } else {
        cnNoRow.classList.add("hidden");
      }
    }
    roundOffRow.classList.remove("hidden");
    $("review-round-off-val").textContent = "₹" + (summary.roundOff || 0).toFixed(2);

    const printedGst = summary.totalGst ?? ((summary.totalCGST || 0) + (summary.totalSGST || 0) + (summary.totalIGST || 0));
    if (printedGstRow) {
      if (printedGst) {
        printedGstRow.classList.remove("hidden");
        let lbl = `₹${printedGst.toFixed(2)}`;
        if (summary.totalCGST || summary.totalSGST || summary.totalIGST) {
          lbl += ` (CGST ₹${(summary.totalCGST || 0).toFixed(2)} + SGST ₹${(summary.totalSGST || 0).toFixed(2)}${summary.totalIGST ? ` + IGST ₹${summary.totalIGST.toFixed(2)}` : ""})`;
        }
        $("review-printed-gst-val").textContent = lbl;
      } else {
        printedGstRow.classList.add("hidden");
      }
    }
    if (printedCdRow) {
      if (summary.cashDiscount) {
        printedCdRow.classList.remove("hidden");
        $("review-printed-cd-val").textContent = "-₹" + summary.cashDiscount.toFixed(2);
      } else {
        printedCdRow.classList.add("hidden");
      }
    }
  } else {
    // Partial / continuation page: any summary block (if present) belongs to
    // the WHOLE invoice, not this page, so it is not asserted here. The
    // declared-total input is this page's own line subtotal, recomputed by
    // recalculate() below, and is saved as partial (invoiceTotal 0) so
    // reporting never double-counts across the pages of one invoice.
    $("review-declared-total-input").value = "0.00";
    cashDiscRow.classList.add("hidden");
    if (schDiscRow) schDiscRow.classList.add("hidden");
    if (cnNoRow) cnNoRow.classList.add("hidden");
    roundOffRow.classList.add("hidden");
    if (printedGstRow) printedGstRow.classList.add("hidden");
    if (printedCdRow) printedCdRow.classList.add("hidden");
  }

  // Pipeline alerts (missing page warning etc.)
  const alertsEl = $("pipeline-alerts");
  alertsEl.innerHTML = "";
  if (extracted.captureQuality?.missingPage) {
    alertsEl.innerHTML = `<div class="bg-amber-500/10 border border-amber-500/30 rounded-lg p-2 text-[10px] text-amber-400 font-semibold">
      ⚠ Possible missing page detected — verify line items are complete.
    </div>`;
  }
  const gstCheck = extracted.gstCheck;
  if (window._footerChecksValid === false) {
    alertsEl.innerHTML += `<div class="bg-indigo-500/10 border border-indigo-500/30 rounded-lg p-2 text-[10px] text-indigo-300 font-semibold">
      ℹ Partial page — line items only (no printed footer totals asserted on this page). Saved as partial; the invoice total is not counted on this page.
    </div>`;
  }
  if (gstCheck && gstCheck.pass === false) {
    const detail = (gstCheck.issues || []).slice(0, 3).map((i) => `• ${i}`).join("<br>");
    alertsEl.innerHTML += `<div class="bg-rose-500/10 border border-rose-500/40 rounded-lg p-2 text-[10px] text-rose-300 font-semibold">
      ⚠ AI GST self-check FAILED after ${gstCheck.attempts || 3} attempt(s). Per-line GST % / ₹ / Net columns may be misread — verify every line below.
      <div class="font-normal mt-1 text-rose-400/90">${detail}</div>
    </div>`;
  }

  // Render line items
  renderLineItems(extracted.lineItems || []);

  // Arithmetic check
  recalculate();

  // Cancel button — keep the invoice in the queue (NOT discarded) so it can be
  // reviewed again later; move it to the end so the one-by-one chain visits the
  // other images first, then advance to the next ready review.
  $("btn-review-cancel").onclick = () => {
    const qid = reviewSession.queueId;
    if (qid) {
      const item = importQueue.find((i) => i.imageId === qid);
      if (item) {
        item.status = "extracted";
        const others = importQueue.filter((i) => i.imageId !== qid);
        others.push(item);
        importQueue.splice(0, importQueue.length, ...others);
      }
    }
    closeReviewPanel();
    showToast("Kept in queue — review later.", "info");
    advanceImportQueue();
  };

  // Reject button — permanent discard: deletes the queue doc + raw image.
  $("btn-review-reject").onclick = async () => {
    const qid = reviewSession.queueId;
    // Pending-invoice reviews have no queueId — clean up their page images here.
    const pathsToDelete = qid ? [] : reviewSession.storagePaths;
    closeReviewPanel();
    showToast("Invoice discarded", "warning");
    if (qid) {
      await discardQueueItem(qid, { silent: true });
    } else if (pathsToDelete && pathsToDelete.length > 0) {
      for (const p of pathsToDelete) {
        try { await deleteObject(ref(storage, p)); } catch (_) {}
      }
    }
    advanceImportQueue();
  };

  // Approve button
  $("btn-review-approve").onclick = () => confirmAndSave(extracted);

  // Expose recalculate globally (called from inline oninput)
  window.triggerRecalculate = recalculate;
}

function renderLineItems(lineItems) {
  const container = $("review-line-items");
  container.innerHTML = "";

  lineItems.forEach((item, idx) => {
    const conf = item.confidence || {};
    const avgConf = avgConfidenceValue(conf);
    const isLowConf = avgConf < 90;
    const fields = [
      { key: "medicineName", label: "Medicine", type: "text", value: item.medicineName },
      { key: "batchNumber", label: "Batch", type: "text", value: item.batchNumber },
      { key: "expiryDate", label: "Expiry", type: "text", value: item.expiryDate },
      { key: "quantityBilled", label: "Qty Billed", type: "number", value: item.quantityBilled },
      { key: "quantityFree", label: "Qty Free", type: "number", value: item.quantityFree },
      { key: "unitPrice", label: "Unit Price ₹", type: "number", value: item.unitPrice },
      { key: "cdPercent", label: "CD %", type: "number", value: item.cdPercent },
      { key: "taxableValue", label: "Taxable ₹", type: "number", value: item.taxableValue },
      { key: "cdValue", label: "CD ₹", type: "number", value: item.cdValue },
      { key: "gstRate", label: "GST %", type: "number", value: item.gstRate },
      { key: "gstValue", label: "GST ₹", type: "number", value: item.gstValue },
      { key: "netValue", label: "Net Value ₹", type: "number", value: item.netValue },
    ];

    const card = document.createElement("div");
    card.className = "bg-slate-800/60 border border-slate-700/60 rounded-xl p-3 space-y-2";
    card.dataset.idx = idx;
    // Store avg confidence as data attribute for reliable reading in recalculate()
    card.dataset.avgConf = avgConf.toFixed(0);

    const nameConf = conf.medicineName ?? 1;
    const nameClass = nameConf < 0.8 ? "text-amber-400" : "text-slate-100";

    card.innerHTML = `
      <div class="flex justify-between items-center">
        <span class="text-[10px] font-bold text-slate-500 uppercase tracking-wider">Line ${idx + 1}</span>
        <span class="text-[9px] px-1.5 py-0.5 rounded ${isLowConf ? "bg-amber-500/20 text-amber-400 border border-amber-500/40" : "bg-emerald-500/10 text-emerald-400"}" data-conf-badge>
          avg conf ${avgConf.toFixed(0)}%
        </span>
      </div>
      ${isLowConf ? `<div class="text-[9px] text-amber-400 font-semibold flex items-center gap-1"><svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" /></svg> Low confidence — review this line before saving</div>` : ""}
      <div class="grid grid-cols-2 gap-1.5">
        ${fields.map((f) => {
          const c = conf[f.key] ?? 1;
          const low = c < 0.8;
          return `
            <div class="space-y-0.5 ${f.key === "medicineName" ? "col-span-2" : ""}">
              <label class="text-[9px] font-bold ${low ? "text-amber-400" : "text-slate-500"} flex items-center gap-1">
                ${f.label}
                ${low ? `<span title="Low confidence: ${(c * 100).toFixed(0)}%">⚠</span>` : ""}
              </label>
              <input
                type="${f.type}"
                step="${f.type === "number" ? "0.01" : ""}"
                data-idx="${idx}"
                data-field="${f.key}"
                value="${f.value ?? ""}"
                class="w-full bg-slate-900 border ${low ? "border-amber-500/50 text-amber-300" : "border-slate-800 text-slate-200"} rounded-lg px-2 py-1.5 text-xs focus:outline-none focus:border-indigo-500 font-mono transition-colors"
                oninput="window.triggerRecalculate()"
              >
            </div>
          `;
        }).join("")}
      </div>
    `;
    container.appendChild(card);
  });
}

// Returns numeric average confidence (0-100) for a confidence object
function avgConfidenceValue(conf) {
  const vals = Object.values(conf).filter((v) => typeof v === "number");
  if (!vals.length) return 100;
  return (vals.reduce((a, b) => a + b, 0) / vals.length) * 100;
}

// Returns formatted string (e.g. "87%") for display
function avgConfidence(conf) {
  return avgConfidenceValue(conf).toFixed(0) + "%";
}

function recalculate() {
  let totalNet = 0;
  let totalGst = 0;
  let totalCd = 0;
  let totalTaxable = 0;

  document.querySelectorAll("#review-line-items [data-field='netValue']").forEach((el) => {
    totalNet += parseFloat(el.value) || 0;
  });
  document.querySelectorAll("#review-line-items [data-field='gstValue']").forEach((el) => {
    totalGst += parseFloat(el.value) || 0;
  });
  document.querySelectorAll("#review-line-items [data-field='cdValue']").forEach((el) => {
    totalCd += parseFloat(el.value) || 0;
  });
  document.querySelectorAll("#review-line-items [data-field='taxableValue']").forEach((el) => {
    totalTaxable += parseFloat(el.value) || 0;
  });

  $("review-subtotal-val").textContent = "₹" + totalNet.toFixed(2);
  $("review-gst-val").textContent = "₹" + totalGst.toFixed(2);

  const footerChecksValid = window._footerChecksValid === true;
  let declaredTotal = parseFloat($("review-declared-total-input").value) || 0;
  let computedTotal, diff, match;

  const summary = window._invoiceSummary;

  // 1) GRAND TOTAL check — printed footer formula (REFERENCE ONLY, non-blocking):
  //    Grand Total = Sale Value − Sch Disc − Cash Disc + Total GST + Round Off − CN.NO
  //    Only asserted when this page IS the complete invoice. On a partial page
  //    the declared total is this page's own line subtotal and matches by
  //    construction — no footer identity is asserted.
  if (!footerChecksValid) {
    computedTotal = totalNet + totalGst;
    $("review-declared-total-input").value = computedTotal.toFixed(2);
  } else if (summary && summary.saleValue) {
    const sVal = summary.saleValue || 0;
    const sch = summary.schDisc || 0;
    const cash = summary.cashDiscount || 0;
    const gstSum = summary.totalGst || totalGst;
    const ro = summary.roundOff || 0;
    const cn = summary.cnNo || 0;
    computedTotal = sVal - sch - cash + gstSum + ro - cn;
  } else {
    computedTotal = totalNet + totalGst;
  }
  declaredTotal = parseFloat($("review-declared-total-input").value) || 0;
  diff = Math.abs(computedTotal - declaredTotal);
  match = diff <= 2;

  if (footerChecksValid) {
    console.log(
      "[recalc] TOTAL check:", {
        gate: footerChecksValid,
        declaredTotal: declaredTotal.toFixed(2),
        computedTotal: (computedTotal || 0).toFixed(2),
        diff: diff.toFixed(2),
        badge: match ? "OK" : "WARN Total " + diff.toFixed(2),
        lineSumNet: totalNet.toFixed(2),
        summary: summary ? {
          saleValue: summary.saleValue,
          schDisc: summary.schDisc,
          cashDiscount: summary.cashDiscount,
          totalGst: summary.totalGst,
          roundOff: summary.roundOff,
          cnNo: summary.cnNo,
        } : null,
      }
    );
  }

  // 2) Independent GST cross-check vs printed footer totals (REFERENCE ONLY,
  //    non-blocking) — only when this page carries trustworthy footer totals.
  const printedGst = summary && (summary.totalGst ?? ((summary.totalCGST || 0) + (summary.totalSGST || 0) + (summary.totalIGST || 0)));
  let gstDiff = 0;
  if (footerChecksValid && printedGst) {
    gstDiff = Math.abs(totalGst - printedGst);
  }

  // 3) Per-line arithmetic checks — WARNING ONLY
  //    a) netValue vs taxableValue + gstValue (catches column swaps)
  //    b) taxableValue vs unitPrice × qtyBilled × (1 - cdPercent/100) (GOLDEN ROW FORMULA — bug #2 from VN-23-341141)
  let arithmeticIssues = [];
  document.querySelectorAll("#review-line-items [data-idx]").forEach((card) => {
    const idx = card.dataset.idx;
    const taxableEl = card.querySelector("[data-field='taxableValue']");
    const gstEl = card.querySelector("[data-field='gstValue']");
    const netEl = card.querySelector("[data-field='netValue']");
    const unitPriceEl = card.querySelector("[data-field='unitPrice']");
    const qtyBilledEl = card.querySelector("[data-field='quantityBilled']");
    const cdPercentEl = card.querySelector("[data-field='cdPercent']");
    const nameEl = card.querySelector("[data-field='medicineName']");
    const name = nameEl ? nameEl.value : `Line ${parseInt(idx) + 1}`;

    // Check a: netValue = taxableValue + gstValue
    if (taxableEl && gstEl && netEl) {
      const taxable = parseFloat(taxableEl.value) || 0;
      const gst = parseFloat(gstEl.value) || 0;
      const net = parseFloat(netEl.value) || 0;
      if (taxable > 0 && gst > 0 && net > 0) {
        const expectedNet = taxable + gst;
        if (Math.abs(net - expectedNet) > 1) {
          arithmeticIssues.push(`"${name}": netValue ₹${net.toFixed(2)} ≠ taxable ₹${taxable.toFixed(2)} + GST ₹${gst.toFixed(2)} = ₹${expectedNet.toFixed(2)} (columns may be swapped)`);
        }
      }
    }

    // Check b: taxableValue ≈ unitPrice × qtyBilled × (1 - cdPercent/100)
    if (taxableEl && unitPriceEl && qtyBilledEl && cdPercentEl) {
      const taxable = parseFloat(taxableEl.value) || 0;
      const unitPrice = parseFloat(unitPriceEl.value) || 0;
      const qtyBilled = parseFloat(qtyBilledEl.value) || 0;
      const cdPercent = parseFloat(cdPercentEl.value) || 0;
      if (unitPrice > 0 && qtyBilled > 0 && taxable > 0) {
        const expectedTaxable = unitPrice * qtyBilled * (1 - cdPercent / 100);
        if (Math.abs(taxable - expectedTaxable) > 2) {
          arithmeticIssues.push(`"${name}": taxableValue ₹${taxable.toFixed(2)} ≠ unitPrice ₹${unitPrice.toFixed(2)} × qty ${qtyBilled} × (1 - ${cdPercent}%) = ₹${expectedTaxable.toFixed(2)} (taxableValue or unitPrice/qty/CD% may be misread)`);
        }
      }
    }
  });

  // 4) Confidence-based gate: any line with avgConf < 90% gets flagged
  let lowConfLines = [];
  document.querySelectorAll("#review-line-items [data-idx]").forEach((card) => {
    const idx = card.dataset.idx;
    const avgConf = parseInt(card.dataset.avgConf || "100", 10);
    if (avgConf < 90) {
      const nameEl = card.querySelector("[data-field='medicineName']");
      const name = nameEl ? nameEl.value : `Line ${parseInt(idx) + 1}`;
      lowConfLines.push({ name, conf: avgConf, idx });
    }
  });

  // Overall gate: block save if ANY low-confidence lines exist (unless acknowledged)
  // The per-line arithmetic and footer mismatches are now WARNINGS only, not blockers.
  const hasLowConf = lowConfLines.length > 0;

  const badge = $("review-arithmetic-badge");
  const gstBadge = $("review-gst-badge");
  const warning = $("arithmetic-warning-banner");
  const warningDetail = $("arithmetic-warning-detail");
  const ackContainer = $("arithmetic-ack-container");
  const approveBtn = $("btn-review-approve");

  // Update badges (reference only)
  styleBadge(badge, match, "✓ Totals Match (ref)", `⚠ Total ₹${diff.toFixed(2)} (ref)`);
  if (gstBadge) {
    if (!footerChecksValid || !printedGst) {
      gstBadge.classList.add("hidden");
    } else {
      gstBadge.classList.remove("hidden");
      styleBadge(gstBadge, gstDiff <= 1, "✓ GST Matches (ref)", `⚠ GST ₹${gstDiff.toFixed(2)} (ref)`);
    }
  }

  if (!hasLowConf) {
    // No low-confidence lines — save is enabled (warnings shown but not blocking)
    warning.classList.add("hidden");
    ackContainer.classList.add("hidden");
    approveBtn.disabled = false;
    approveBtn.className = "py-2.5 bg-indigo-600 text-white font-bold rounded-lg text-xs hover:bg-indigo-500 transition-all active:scale-[0.98]";
  } else {
    // Low-confidence lines exist — block save until acknowledged
    warning.classList.remove("hidden");
    if (warningDetail) {
      const fails = [];
      if (footerChecksValid && !match) fails.push(`Grand total mismatch ₹${diff.toFixed(2)} (reference)`);
      if (footerChecksValid && printedGst && gstDiff > 1) fails.push(`GST: sum ₹${totalGst.toFixed(2)} ≠ printed ₹${printedGst.toFixed(2)} (reference)`);
      if (arithmeticIssues.length > 0) fails.push(...arithmeticIssues);
      if (lowConfLines.length > 0) {
        fails.push(`⚠ ${lowConfLines.length} line(s) below 90% confidence — review required`);
        lowConfLines.forEach(lc => fails.push(`  • ${lc.name}: ${lc.conf}%`));
      }
      warningDetail.textContent = fails.join(" · ");
    }
    ackContainer.classList.remove("hidden");
    const ackCheckbox = $("arithmetic-ack-checkbox");
    approveBtn.disabled = !ackCheckbox.checked;
    if (ackCheckbox.checked) {
      approveBtn.className = "py-2.5 bg-amber-600 text-white font-bold rounded-lg text-xs hover:bg-amber-500 transition-all active:scale-[0.98]";
    } else {
      approveBtn.className = "py-2.5 bg-slate-700 text-slate-500 font-bold rounded-lg text-xs cursor-not-allowed";
    }
  }
}

function styleBadge(el, ok, okText, failText) {
  if (ok) {
    el.textContent = okText;
    el.className = "px-2 py-0.5 text-[9px] rounded font-bold uppercase tracking-wider bg-emerald-500/15 text-emerald-400 border border-emerald-500/30";
  } else {
    el.textContent = failText;
    el.className = "px-2 py-0.5 text-[9px] rounded font-bold uppercase tracking-wider bg-rose-500/15 text-rose-400 border border-rose-500/30";
  }
}

// ─── Confirm & Save ───────────────────────────────────────────────────────────

async function confirmAndSave(originalExtracted) {
  console.log("[Save] confirmAndSave called", originalExtracted && { invoiceNumber: originalExtracted.invoiceNumber, distributor: originalExtracted.distributor });

  try {
    // Build a map of user edits from DOM (only for fields the user actually edited)
    const domEdits = new Map();
    document.querySelectorAll("#review-line-items [data-idx]").forEach((card) => {
      const idx = parseInt(card.dataset.idx);
      const edits = {};
      card.querySelectorAll("[data-field]").forEach((input) => {
        edits[input.dataset.field] = input.value;
      });
      domEdits.set(idx, edits);
    });

    // Start with the FULL extraction result — this guarantees all 17 (or however many)
    // line items are preserved even if some aren't currently rendered in DOM.
    const extractedLineItems = originalExtracted.lineItems || [];
    const expectedCount = extractedLineItems.length;

    // Provenance fields (Priority 3) — trace every medicine record back to its source invoice
    const provenance = {
      invoiceNumber: originalExtracted.invoiceNumber || "",
      distributorId: normalizeDistributorName(originalExtracted.distributor || ""),
      invoiceDate: originalExtracted.invoiceDate || "",
      // invoiceId will be added by backend after invoice doc is created
    };

    // Merge DOM edits onto the full extraction array, and attach provenance
    const lineItems = extractedLineItems.map((item, idx) => {
      const edits = domEdits.get(idx);
      const base = edits ? {
        ...item,
        medicineName: edits.medicineName ?? item.medicineName,
        batchNumber: edits.batchNumber ?? item.batchNumber,
        expiryDate: edits.expiryDate ?? item.expiryDate,
        quantityBilled: numOr(edits.quantityBilled, item.quantityBilled),
        quantityFree: numOr(edits.quantityFree, item.quantityFree),
        unitPrice: numOr(edits.unitPrice, item.unitPrice),
        cdPercent: numOr(edits.cdPercent, item.cdPercent),
        taxableValue: numOr(edits.taxableValue, item.taxableValue),
        cdValue: numOr(edits.cdValue, item.cdValue),
        gstRate: numOr(edits.gstRate, item.gstRate),
        gstValue: numOr(edits.gstValue, item.gstValue),
        netValue: numOr(edits.netValue, item.netValue),
      } : item;

      // Attach provenance to every line item
      return { ...base, ...provenance };
    });

    // HARD GUARD: Never let a partial save succeed silently.
    // If the final array length doesn't match the extraction, block and alert.
    if (lineItems.length !== expectedCount) {
      throw new Error(
        `Save blocked: line item count mismatch. Extraction had ${expectedCount} items, ` +
        `but save pipeline produced ${lineItems.length}. This indicates a bug — ` +
        `please report this and do not proceed.`
      );
    }

    const invoiceTotal = parseFloat($("review-declared-total-input").value) || 0;
    console.log("[Save] prepared for write", { itemCount: lineItems.length, expectedCount, invoiceTotal, authUid: currentUser ? currentUser.uid : null });

    if (!currentUser) {
      throw new Error("Not signed in. Please log in again and retry.");
    }

    console.log("[Save] calling saveInvoice callable...");
    const saveFn = httpsCallable(functions, "saveInvoice");
    const payload = sanitizeForCallable({
      pharmacyId: currentPharmacyId,
      queueId: reviewSession.queueId || null,
      invoice: {
        distributor: originalExtracted.distributor || "",
        invoiceNumber: originalExtracted.invoiceNumber || "",
        invoiceDate: originalExtracted.invoiceDate || "",
        // A partial (continuation) page carries no authoritative total — the
        // server writes invoiceTotal 0 + partial=true so reporting only sums
        // the page(s) that actually hold the printed footer totals.
        invoiceTotal: window._footerChecksValid === true ? invoiceTotal : null,
        invoiceSummary: originalExtracted.invoiceSummary || {},
        captureQuality: originalExtracted.captureQuality || {},
        gstCheck: originalExtracted.gstCheck || {},
        rawGeminiResponse: originalExtracted.rawGeminiResponse || "",
        pHash: reviewSession.pHash || null,
        hasFooterTotals: originalExtracted.hasFooterTotals === true,
        pageNumber: Number(originalExtracted.pageNumber) || 1,
        totalPages: Number(originalExtracted.totalPages) || 1,
        looksLikeContinuationPage: originalExtracted.looksLikeContinuationPage === true,
      },
      lineItems,
      confirmedBy: currentUser.uid,
    });

    // Capture review-session refs BEFORE the panel closes (closeReviewPanel
    // resets reviewSession), then close + advance IMMEDIATELY — the owner is
    // never blocked on the "Saving to Firestore..." overlay. The save runs in
    // the background and toasts when it resolves; on failure the item is put
    // back in the queue for a retry.
    const qid = reviewSession.queueId;
    const stagedNumber = reviewSession.pendingInvoiceNumber;
    const rawPaths = (reviewSession.storagePaths || []).slice();
    let removedItem = null;
    if (qid) {
      const idx = importQueue.findIndex((i) => i.imageId === qid);
      if (idx >= 0) removedItem = importQueue[idx];
      importQueue = importQueue.filter((i) => i.imageId !== qid);
      if (importQueueCursor >= importQueue.length) {
        importQueueCursor = Math.max(0, importQueue.length - 1);
      }
    }
    closeReviewPanel();
    advanceImportQueue();

    (async () => {
      try {
        const resp = await saveFn(payload);
        console.log("[Save] saveInvoice callable succeeded", resp && resp.data);
        if (rawPaths.length > 0) {
          for (const p of rawPaths) {
            try {
              await deleteObject(ref(storage, p));
            } catch (delErr) {
              console.warn("Could not delete raw file (non-fatal):", delErr.message);
            }
          }
        }
        if (stagedNumber) {
          try {
            const delFn = httpsCallable(functions, "deletePendingInvoice", { timeout: 30000 });
            await delFn({ pharmacyId: currentPharmacyId, invoiceNumber: stagedNumber });
          } catch (cleanupErr) {
            console.warn("[Save] could not remove staged invoice (non-fatal):", cleanupErr.message);
          }
        }
        if (resp.data && resp.data.duplicateWarning) {
          const w = resp.data.duplicateWarning;
          showToast(
            `Saved! ${lineItems.length} medicine(s). Note: invoice ${w.invoiceNumber} from ${w.distributor} was already saved — this may be a duplicate page or a re-upload.`,
            "warning"
          );
        } else {
          showToast(`Saved! ${lineItems.length} medicine(s) recorded.`, "success");
        }
        console.log("[Save] done — saved in background while the next review was shown");
      } catch (err) {
        console.error("[Save] FAILED", {
          code: err && err.code,
          message: err && err.message,
          details: err && err.details,
          error: err,
        });
        if (removedItem) {
          importQueue.push(removedItem);
          renderImportQueueStatus();
        }
        showToast("Save failed: " + (err && err.message ? err.message : String(err)), "error");
      }
    })();

  } catch (err) {
    console.error("[Save] FAILED", {
      code: err && err.code,
      message: err && err.message,
      details: err && err.details,
      error: err,
    });
    showLoadingOverlay(false);
    showToast("Save failed: " + (err && err.message ? err.message : String(err)), "error");
  }
}

function revokeReviewSession() {
  if (reviewSession.objectUrls && reviewSession.objectUrls.length > 0) {
    reviewSession.objectUrls.forEach(url => URL.revokeObjectURL(url));
  }
  reviewSession = { storagePaths: [], objectUrls: [], currentPageIndex: 0, fileType: 'image', extracted: null, queueId: null, pendingInvoiceNumber: null };
}

// Hard-close the review drawer: guarantees the panel is gone (inline display
// beats any CSS ordering issue), blanks the old image/PDF sources so a reused
// element never flashes the previously-saved invoice, and revokes object URLs.
// Used after save AND discard so the extracted-data screen never lingers.
function closeReviewPanel() {
  const panel = $("extraction-review-panel");
  if (panel) {
    panel.classList.add("hidden");
    panel.style.display = "none";
  }
  const imgEl = $("review-invoice-img");
  if (imgEl) imgEl.removeAttribute("src");
  const pdfEl = $("review-invoice-pdf");
  if (pdfEl) pdfEl.removeAttribute("src");
  revokeReviewSession();
}

// ─── Expiring Medicines List (live) ──────────────────────────────────────────
// Real-time onSnapshot subscription on the same "medicines" collection the
// saveInvoice CF writes (the flattened aggregate of ALL saved invoices) — the
// widget updates instantly when an invoice is confirmed, without a page
// refresh. No query limit: every batch in the collection is counted and
// rendered, so the total reflects the true database count.

let expiringUnsub = null;

function subscribeExpiringMedicines() {
  if (expiringUnsub) return;
  const listEl = $("expiring-list");
  if (!listEl) return;

  expiringUnsub = onSnapshot(
    collection(db, "pharmacies", currentPharmacyId, "medicines"),
    (snapshot) => {
      const now = new Date();
      const ninetyDaysOut = new Date(now.getTime() + 90 * 24 * 60 * 60 * 1000);
      const withDate = [];
      const noDate = [];

      snapshot.forEach((docSnap) => {
        const d = docSnap.data();
        const expDate = parseExpiryDate(d.expiryDate);
        const row = { id: docSnap.id, ...d, expDate };
        if (expDate) withDate.push(row);
        else noDate.push(row);
      });

      withDate.sort((a, b) => a.expDate - b.expDate);
      const items = [...withDate, ...noDate];

      // True database count — every batch stored in the medicines collection,
      // not just the expiring subset.
      const countEl = $("expiring-count");
      if (countEl) countEl.textContent = `${items.length} record${items.length !== 1 ? "s" : ""}`;

      if (items.length === 0) {
        listEl.innerHTML = `<div class="text-center py-6 text-xs text-slate-500">No batches recorded yet. Save an invoice to populate.</div>`;
        return;
      }

      listEl.innerHTML = items.map((item) => {
        const daysLeft = item.expDate ? Math.ceil((item.expDate - now) / (1000 * 60 * 60 * 24)) : null;
        const urgency = !item.expDate
          ? "border-slate-700/60 bg-slate-800/40"
          : daysLeft <= 30 ? "border-rose-500/40 bg-rose-500/5"
          : daysLeft <= 60 ? "border-amber-500/40 bg-amber-500/5"
          : daysLeft <= 90 ? "border-amber-500/30 bg-amber-500/5"
          : "border-slate-700/60 bg-slate-800/40";
        const badgeClass = !item.expDate
          ? "bg-slate-700 text-slate-400"
          : daysLeft <= 0 ? "bg-rose-500/20 text-rose-400"
          : daysLeft <= 30 ? "bg-rose-500/20 text-rose-400"
          : daysLeft <= 60 ? "bg-amber-500/20 text-amber-400"
          : daysLeft <= 90 ? "bg-amber-500/15 text-amber-400"
          : "bg-slate-700 text-slate-400";
        const badge = !item.expDate
          ? "NO EXPIRY"
          : daysLeft <= 0 ? "EXPIRED" : `${daysLeft}d left`;
        return `
          <div class="border ${urgency} rounded-xl p-3 space-y-1.5">
            <div class="flex justify-between items-start">
              <div>
                <p class="text-xs font-bold text-slate-100 leading-tight">${item.medicineName}</p>
                <p class="text-[9px] font-mono text-slate-500">Batch: ${item.batchNumber}</p>
              </div>
              <span class="text-[9px] px-2 py-0.5 rounded-full font-bold ${badgeClass}">
                ${badge}
              </span>
            </div>
            <div class="flex items-center justify-between text-[10px] text-slate-400">
              <span>Exp: ${item.expiryDate || "—"}</span>
              <span>Qty: ${item.remainingQty ?? item.quantityBilled}</span>
              <span class="text-slate-500">${normalizeDistributorName(item.distributor) || "—"}</span>
            </div>
          </div>
        `;
      }).join("");
    },
    (err) => {
      listEl.innerHTML = `<div class="text-center py-6 text-xs text-rose-400">Error loading: ${err.message}</div>`;
    }
  );
}

// Tolerant expiry-date parser covering every format the Gemini pipeline may
// have stored: MM/YYYY, MM-YYYY, MM.YYYY, YYYY-MM, DD/MM/YYYY, DD-MM-YYYY,
// DD.MM.YYYY, MM/YY, MMM-YYYY, bare YYYY, and any Date-parseable string.
// Records whose expiry fails to parse are surfaced as "NO EXPIRY" instead of
// silently vanishing from the dashboard count.
function parseExpiryDate(str) {
  if (str == null) return null;
  const s = String(str).trim();
  if (!s) return null;
  let m;

  // YYYY-MM-DD / YYYY/MM/DD / YYYY.MM.DD
  m = s.match(/^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$/);
  if (m) return new Date(parseInt(m[1]), parseInt(m[2]) - 1, parseInt(m[3]));

  // DD-MM-YYYY / DD/MM/YYYY / DD.MM.YYYY (Indian invoices are day-first)
  m = s.match(/^(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})$/);
  if (m && parseInt(m[1]) <= 31 && parseInt(m[2]) <= 12) {
    return new Date(parseInt(m[3]), parseInt(m[2]) - 1, parseInt(m[1]));
  }

  // MM-YYYY / MM/YYYY / MM.YYYY / MM YYYY
  m = s.match(/^(\d{1,2})[-/.\s](\d{4})$/);
  if (m) return new Date(parseInt(m[2]), parseInt(m[1]) - 1, 28);

  // YYYY-MM / YYYY/MM
  m = s.match(/^(\d{4})[-/.](\d{1,2})$/);
  if (m) return new Date(parseInt(m[1]), parseInt(m[2]) - 1, 28);

  // MM/YY short year (e.g. 05/27)
  m = s.match(/^(\d{1,2})[-/.](\d{2})$/);
  if (m) return new Date(parseInt(m[2]) + 2000, parseInt(m[1]) - 1, 28);

  // MMM-YYYY / MMM YYYY / MMM.YYYY (e.g. MAY 2027, "MAY-27")
  m = s.match(/^([a-z]{3})[-/.\s]+(\d{2,4})$/i);
  if (m) {
    const months = { jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5, jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11 };
    const idx = months[m[1].toLowerCase().slice(0, 3)];
    if (idx !== undefined) {
      const y = m[2].length === 2 ? parseInt(m[2]) + 2000 : parseInt(m[2]);
      return new Date(y, idx, 28);
    }
  }

  // Bare year (e.g. 2027)
  m = s.match(/^(\d{4})$/);
  if (m) return new Date(parseInt(m[1]), 11, 31);

  // Last resort: native Date.parse
  const t = Date.parse(s);
  if (!isNaN(t)) return new Date(t);
  return null;
}

// ─── Distributors (live) ──────────────────────────────────────────────────────
// Groups by a normalized distributor key (mirror of the backend
// normalizeDistributorName) so legacy rows already fragmented across casing /
// punctuation / legal-suffix variants also consolidate into one profile.

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

function normalizeDistributorName(name) {
  if (name == null) return "";
  let s = String(name)
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!s) return "";
  let prev = null;
  while (s && s !== prev) {
    prev = s;
    let stripped = false;
    for (const suffix of DISTRIBUTOR_LEGAL_SUFFIXES) {
      const re = new RegExp(`(?:^|\\s)${suffix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "i");
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

let distributorsUnsub = null;

function subscribeDistributors() {
  if (distributorsUnsub) return;
  const listEl = $("distributors-list");
  if (!listEl) return;

  distributorsUnsub = onSnapshot(
    collection(db, "pharmacies", currentPharmacyId, "medicines"),
    (snapshot) => {
      const distributorMap = {};
      snapshot.forEach((docSnap) => {
        const d = docSnap.data();
        const name = normalizeDistributorName(d.distributor);
        if (name) {
          distributorMap[name] = (distributorMap[name] || 0) + 1;
        }
      });

      const distributors = Object.entries(distributorMap);
      if (distributors.length === 0) {
        listEl.innerHTML = `<div class="text-center py-8 text-xs text-slate-500">No distributors recorded yet.</div>`;
        return;
      }

      listEl.innerHTML = distributors.map(([name, count]) => `
        <div class="bg-slate-800/60 border border-slate-700/60 rounded-xl p-3 flex justify-between items-center">
          <div>
            <p class="text-xs font-bold text-slate-100">${name}</p>
            <p class="text-[9px] text-slate-400">${count} medicine batch${count !== 1 ? "es" : ""}</p>
          </div>
          <div class="w-8 h-8 rounded-lg bg-indigo-600/20 flex items-center justify-center">
            <svg class="w-4 h-4 text-indigo-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4"/>
            </svg>
          </div>
        </div>
      `).join("");
    },
    (err) => {
      listEl.innerHTML = `<div class="text-xs text-rose-400 text-center py-4">${err.message}</div>`;
    }
  );
}

// ─── Pending Invoices (Staging, legacy) ─────────────────────────────────────
// Legacy widget kept for backward compatibility. The extraction pipeline no
// longer writes to pending_invoices (every image now goes straight to the
// import queue as "extracted"), so this subscription simply shows an empty
// list unless older staged invoices still exist in the collection.

let pendingUnsub = null;

function subscribePendingInvoices() {
  if (pendingUnsub) return;
  const listEl = $("pending-invoices-list");
  if (!listEl) return;

  const q = query(
    collection(db, "pharmacies", currentPharmacyId, "pending_invoices"),
    orderBy("createdAt", "desc"),
    limit(50)
  );

  pendingUnsub = onSnapshot(
    q,
    (snapshot) => {
      const pending = [];
      snapshot.forEach((docSnap) => {
        const d = docSnap.data();
        const pages = d.pages || [];
        const pageTotal = Math.max(...pages.map((p) => Number(p.totalPages) || 1), 1);
        const pageNumbers = pages
          .map((p) => Number(p.pageNumber))
          .filter((n) => n >= 1)
          .sort((a, b) => a - b);
        pending.push({
          id: docSnap.id,
          invoiceNumber: d.invoiceNumber || docSnap.id,
          distributor: d.distributor || "",
          pageCount: pages.length,
          pageTotal,
          pageNumbers,
          hasFooter: pages.some((p) => p.hasFooterTotals),
          totalItems: pages.reduce((sum, p) => sum + (p.lineItems || []).length, 0),
          status: d.status || "Incomplete - Waiting for remaining pages",
          createdAt: d.createdAt,
        });
      });

      const countEl = $("pending-invoices-count");
      if (countEl) countEl.textContent = `${pending.length} waiting`;
      if (pending.length === 0) {
        listEl.innerHTML = `<div class="text-center py-3 text-[10px] text-slate-500">No partial invoices staged.</div>`;
        return;
      }

      listEl.innerHTML = pending.map((p) => {
        const pagesLabel = p.pageNumbers && p.pageNumbers.length
          ? p.pageNumbers.join(", ")
          : `${p.pageCount} page(s)`;
        const pageInfo = (p.pageTotal && p.pageTotal > 1)
          ? `${pagesLabel} of ${p.pageTotal}`
          : pagesLabel;
        return `
        <div class="bg-amber-500/5 border border-amber-500/20 rounded-xl p-3 flex justify-between items-center">
          <div>
            <p class="text-xs font-bold text-slate-100">${p.invoiceNumber}</p>
            <p class="text-[9px] text-slate-400">${normalizeDistributorName(p.distributor) || "Unknown"} · Pages ${pageInfo} · ${p.totalItems} item(s)</p>
          </div>
          <div class="flex gap-1.5 shrink-0">
            <button onclick="window.openPendingReview('${p.invoiceNumber.replace(/'/g, "")}')" class="text-[9px] px-2 py-1 bg-indigo-500/15 border border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/30 rounded-md font-bold transition-all">
              Review
            </button>
            <button onclick="window.discardPending('${p.invoiceNumber.replace(/'/g, "")}')" class="text-[9px] px-2 py-1 bg-slate-800 border border-slate-700 text-slate-400 hover:text-rose-400 hover:border-rose-500/30 rounded-md font-bold transition-all">
              Discard
            </button>
          </div>
        </div>`;
      }).join("");
    },
    (err) => {
      listEl.innerHTML = `<div class="text-center py-3 text-[10px] text-rose-400">${err.message}</div>`;
    }
  );
}

window.discardPending = async (invoiceNumber) => {
  try {
    const delFn = httpsCallable(functions, "deletePendingInvoice", { timeout: 30000 });
    await delFn({ pharmacyId: currentPharmacyId, invoiceNumber });
    showToast(`Discarded ${invoiceNumber}`, "warning");
    // The live pending_invoices subscription already removed the card.
  } catch (err) {
    showToast("Failed to discard: " + err.message, "error");
  }
};

// Open a staged (partial) invoice from the pending_invoices widget in the full
// review panel so it can be verified and confirmed/saved manually.
window.openPendingReview = async (invoiceNumber) => {
  try {
    showLoadingOverlay(true, "Loading staged invoice...");
    const getFn = httpsCallable(functions, "getPendingInvoice", { timeout: 30000 });
    const resp = await getFn({ pharmacyId: currentPharmacyId, invoiceNumber });
    const data = resp.data;

    // Resolve one object URL per buffered page so review pagination works.
    const objectUrls = [];
    const storagePaths = [];
    for (const pg of data.pages || []) {
      if (!pg.storagePath) continue;
      storagePaths.push(pg.storagePath);
      try {
        const url = await getDownloadURL(ref(storage, pg.storagePath));
        if (url) objectUrls.push(url);
      } catch (_) {}
    }

    reviewSession = {
      storagePaths,
      objectUrls,
      currentPageIndex: 0,
      fileType: (storagePaths[0] || "").endsWith(".pdf") ? "pdf" : "image",
      extracted: data,
      queueId: null,
      pHash: null,
      pendingInvoiceNumber: invoiceNumber,
    };

    // Legacy merged-pending invoices already combine ALL pages + the footer
    // summary, so treat them as a complete single invoice for validation.
    data.hasFooterTotals = true;
    data.looksLikeContinuationPage = false;
    data.pageNumber = 1;
    data.totalPages = 1;

    openReviewPanel(data);
    showLoadingOverlay(false);
    showToast(
      `Staged invoice opened — ${data.lineItems.length} item(s) from ${data.pageCount} page(s). Review & confirm to save.`,
      "info"
    );
  } catch (err) {
    showLoadingOverlay(false);
    showToast("Failed to open staged invoice: " + (err.message || err), "error");
  }
};

// ─── Bootstrap ────────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  // Show auth screen initially
  $("app-workspace").classList.add("hidden");
  $("auth-screen").classList.remove("hidden");
  initAuth();
});

// ─── Import Queue (persisted, resumable) ────────────────────────────────────
// Every selected raw file is uploaded to Storage and immediately recorded as a
// queue doc at /pharmacies/{pharmacyId}/importQueue/{imageId} with
// status "uploaded" BEFORE any AI work. If the browser closes mid-batch, the
// docs survive; the resume loop below picks up any item whose status is not yet
// terminal (saved/reviewed/ingested/failed/rejected) on the next page load.
// The extraction itself runs server-side in the processImportQueueItem CF
// (leased claim, crash-safe); this client loop only enqueues and drives review.

function queueItemRef(imageId) {
  return doc(db, "pharmacies", currentPharmacyId, "importQueue", imageId);
}

// Queue-doc writes are routed through the mutateImportQueue callable because
// the Firestore backend has been observed enforcing stale rules that deny
// direct client writes to the importQueue subtree (Admin SDK bypasses rules).
async function mutateImportQueue(op, imageId, data) {
  const fn = httpsCallable(functions, "mutateImportQueue", { timeout: 30000 });
  const res = await fn({
    op,
    pharmacyId: currentPharmacyId,
    imageId,
    data,
  });
  return res.data;
}

function esc(str) {
  return String(str == null ? "" : str).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

// parseFloat that never yields NaN: empty/garbage input falls back to the
// provided default. (NaN ?? default does NOT fall back — NaN is not nullish.)
function numOr(v, fallback) {
  const n = typeof v === "string" ? parseFloat(v) : Number(v);
  return Number.isFinite(n) ? n : fallback;
}

// Deep-clean a payload before it goes through httpsCallable: JSON cannot
// represent NaN/Infinity, so replace any non-finite number with null.
function sanitizeForCallable(obj) {
  if (Array.isArray(obj)) return obj.map(sanitizeForCallable);
  if (obj && typeof obj === "object") {
    const out = {};
    for (const k of Object.keys(obj)) {
      const v = obj[k];
      if (typeof v === "number" && !Number.isFinite(v)) out[k] = null;
      else if (v && typeof v === "object") out[k] = sanitizeForCallable(v);
      else out[k] = v;
    }
    return out;
  }
  return obj;
}

// Upload a raw file and persist the "uploaded" queue doc BEFORE extraction.
// hints = { pageNumber, totalPages } — authoritative pagination captured from
// the source file metadata (e.g. pdf.numPages for split PDF pages) and stored
// on the queue doc so the worker can never misclassify a multi-page invoice.
async function enqueueFile(file, fileIndex = 0, hints = null) {
  const ext = file.type === "application/pdf" ? "pdf" : "jpg";
  const imageId = `${Date.now()}_${fileIndex}_${Math.random().toString(36).slice(2)}`;
  const storagePath = `invoices/${currentPharmacyId}/${imageId}.${ext}`;
  const storageRef = ref(storage, storagePath);

  // Layer 1: pHash duplicate check (soft warning, not a hard block)
  let pHash = null;
  let duplicateWarning = null;
  if (file.type.startsWith("image/")) {
    try {
      console.log("[enqueueFile] Starting pHash duplicate check for", file.name);
      const dupCheck = await checkDuplicateByHash(file, currentPharmacyId);
      console.log("[enqueueFile] pHash check result:", dupCheck);
      pHash = dupCheck.newHash;
      if (dupCheck.isDuplicate) {
        const m = dupCheck.match;
        const dateStr = m.createdAt?.toDate ? m.createdAt.toDate().toLocaleDateString() : "recently";
        duplicateWarning = `⚠ This image looks like invoice ${m.invoiceNumber} from ${m.distributor || "unknown distributor"} (uploaded ${dateStr}, hash distance ${m.distance}). Proceed anyway?`;
        if (!confirm(duplicateWarning)) {
          throw new Error("Upload cancelled by user (duplicate detected).");
        }
      }
    } catch (err) {
      if (err.message?.includes("cancelled")) throw err;
      console.warn("[pHash] duplicate check failed (non-fatal):", err.message, err);
    }
  }

  console.log("[enqueueFile] Uploading to Storage:", storagePath);
  await uploadBytes(storageRef, file, { contentType: file.type });
  console.log("[enqueueFile] Storage upload complete, writing queue doc");
  const queueRef = queueItemRef(imageId);
  
  // Diagnostic: check actual auth state and app config
  console.log("[enqueueFile] Diagnostic - app.options.projectId:", app.options.projectId);
  console.log("[enqueueFile] Diagnostic - auth.currentUser:", auth.currentUser?.uid, "vs app currentUser:", currentUser?.uid, "providers:", auth.currentUser?.providerData?.map(p => p.providerId), "phone:", auth.currentUser?.phoneNumber);
  try {
    const token = await auth.currentUser?.getIdToken();
    console.log("[enqueueFile] Diagnostic - getIdToken() succeeded:", !!token, "length:", token?.length);
    if (token) {
      const parts = token.split(".");
      if (parts.length === 3) {
        const payload = JSON.parse(atob(parts[1].replace(/-/g, "+").replace(/_/g, "/")));
        console.log("[enqueueFile] Diagnostic - token payload:", { iss: payload.iss, aud: payload.aud, sub: payload.sub, exp: payload.exp, iat: payload.iat, auth_time: payload.auth_time, firebase: payload.firebase });
      }
    }
  } catch (tokenErr) {
    console.error("[enqueueFile] Diagnostic - getIdToken() FAILED:", tokenErr.code, tokenErr.message);
  }
  
  console.log("[enqueueFile] About to write:", queueRef.path, "auth.uid:", currentUser?.uid, "currentPharmacyId:", currentPharmacyId);
  try {
    await mutateImportQueue("create", imageId, {
      imageId,
      storagePath,
      fileName: file.name,
      status: "uploaded",
      pdfPageNumber: hints && Number(hints.pageNumber) >= 1 ? Number(hints.pageNumber) : null,
      pdfTotalPages: hints && Number(hints.totalPages) >= 1 ? Number(hints.totalPages) : null,
      pHash: pHash || null,
      duplicateWarning: duplicateWarning || null,
    });
  } catch (writeErr) {
    console.error("[enqueueFile] mutateImportQueue FAILED - code:", writeErr.code, "message:", writeErr.message, "details:", writeErr.details, "full error:", JSON.stringify(writeErr, Object.getOwnPropertyNames(writeErr)));
    throw writeErr;
  }
  console.log("[enqueueFile] Queue doc written:", imageId);

  return {
    imageId,
    storagePath,
    fileName: file.name,
    status: "uploaded",
    retries: 0,
    pollCount: 0,
    pdfPageNumber: hints && Number(hints.pageNumber) >= 1 ? Number(hints.pageNumber) : null,
    pdfTotalPages: hints && Number(hints.totalPages) >= 1 ? Number(hints.totalPages) : null,
    pHash,
    uploadedAt: new Date(),
    file,
  };
}

async function handleMultipleFilesSelected(entries) {
  console.log("[handleMultipleFilesSelected] Starting batch upload:", entries.length, "files");
  showLoadingOverlay(true, `Uploading ${entries.length} file(s)...`);
  const items = [];
  for (let i = 0; i < entries.length; i++) {
    try {
      // PDF split pages arrive as { file, pageNumber, totalPages }; gallery
      // files arrive as raw File objects.
      const entry = entries[i];
      const file = entry instanceof File ? entry : entry.file;
      const hints = entry instanceof File ? null : { pageNumber: entry.pageNumber, totalPages: entry.totalPages };
      console.log(`[handleMultipleFilesSelected] Uploading file ${i+1}/${entries.length}:`, file.name);
      items.push(await enqueueFile(file, i, hints));
    } catch (err) {
      console.error(`[handleMultipleFilesSelected] Upload FAILED for file ${i}:`, err.code, err.message, err);
      showToast(`Upload failed for ${entries[i].name || "file"}: ${err.message}`, "error");
    }
  }
  showLoadingOverlay(false);
  console.log("[handleMultipleFilesSelected] Batch complete, successful:", items.length);
  if (items.length > 0) {
    importQueue.push(...items);
    importQueueAuto = true;
    renderImportQueueStatus();
    startImportQueue();
  }
}

function startImportQueue() {
  if (importQueueBusy) return;
  importQueueBusy = true;
  processNextQueueItem();
}

// Open the next "Ready for review" item, chaining one-by-one. Returns false
// (and clears the chain) when nothing is left to review.
function tryOpenNextReview() {
  const panel = $("extraction-review-panel");
  if (panel && !panel.classList.contains("hidden")) return false; // a review is already open
  const next = importQueue.find((i) => i.status === "extracted" && i.extracted);
  if (!next) {
    reviewChainActive = false;
    renderImportQueueStatus();
    return false;
  }
  reviewChainActive = true;
  openQueueReview(next, next.extracted);
  return true;
}

async function processNextQueueItem() {
  // Phase 1 — extract EVERY queued item in the background (no review panel).
  // This is what makes the whole batch "Ready for review" as soon as the upload
  // finishes, instead of stopping at the first image like the old flow did
  // (which left every later item stuck at "Queued").
  while (importQueueCursor < importQueue.length) {
    const item = importQueue[importQueueCursor];
    renderImportQueueStatus();
    if (item.status === "extracted") {
      importQueueCursor++;
      continue; // already ready — never block the batch on a review panel
    }
    const outcome = await processQueueItem(item, false);
    if (outcome === "retry") {
      renderImportQueueStatus();
      continue; // transient — re-attempt the same item
    }
    importQueueCursor++;
  }
  importQueueBusy = false;

  // Phase 2 — once everything is ready (or on resume), open the first item so
  // the owner reviews the images one by one with no extraction wait.
  tryOpenNextReview();
}

// Move on from a finished/saved/cancelled/discarded item: re-extract anything
// still queued, then open the next ready review. On a live batch this chains
// straight through every image; on resume it re-opens the first ready item.
function advanceImportQueue() {
  startImportQueue();
}

async function processQueueItem(item, openReview = true) {
  // Fast paths that need no re-extraction (resume-safe).
  let qData = null;
  try {
    const qSnap = await getDoc(queueItemRef(item.imageId));
    qData = qSnap.exists ? qSnap.data() : null;
  } catch (_) {}
  if (qData) {
    const qs = qData.status || "";
    if (qs === "extracted" && qData.extracted) {
      item.status = "extracted";
      const extracted = qData.extracted;
      item.extracted = extracted;
      if (openReview) {
        await openQueueReview(item, extracted);
        return "awaiting-review";
      }
      return "done";
    }
    if (qs === "ingested" || qs === "ingested-partial") {
      item.status = qs;
      return "done"; // merged already, or a staged page awaiting its footer page
    }
    if (["saved", "reviewed", "failed", "rejected"].includes(qs)) {
      item.status = qs;
      return "done";
    }
  }

  // Let the server claim (leased) and extract. Reflect the live status in the
  // queue list immediately so the owner sees "Processing…" instead of "Queued"
  // while Gemini runs.
  item.status = "processing";
  renderImportQueueStatus();
  console.log("[processQueueItem] Calling processImportQueueItem for", item.imageId, { pharmacyId: currentPharmacyId, pdfPageNumber: item.pdfPageNumber, pdfTotalPages: item.pdfTotalPages });
  const processFn = httpsCallable(functions, "processImportQueueItem", { timeout: 120000 });
  let result;
  try {
    result = await processFn({
      pharmacyId: currentPharmacyId,
      imageId: item.imageId,
      pdfPageNumber: item.pdfPageNumber || null,
      pdfTotalPages: item.pdfTotalPages || null,
    });
    console.log("[processQueueItem] CF response:", result.data);
  } catch (err) {
    console.error("[processQueueItem] CF ERROR:", err.code, err.message, err.details, err);
    showToast(`Queue processing error: ${err.message}`, "error");
    return "done";
  }
  const data = result.data || {};
  const status = data.status || "unknown";

  if (status === "extracted") {
    item.status = "extracted";
    const extracted = data.extracted || {};
    item.extracted = extracted;
    if (openReview) {
      await openQueueReview(item, extracted);
      return "awaiting-review";
    }
    return "done";
  }
  if (status === "ingested") {
    item.status = "ingested";
    showToast(data.message || "Invoice merged & saved.", "success");
    return "done";
  }
  if (status === "ingested-partial" || status === "staged") {
    item.status = "ingested-partial";
    showToast(data.message || "Page staged — waiting for remaining pages.", "info");
    return "done";
  }
  if (status === "uploaded") {
    // Transient failure — the server reverted the item for a retry.
    item.retries = (item.retries || 0) + 1;
    if (item.retries >= 3) {
      item.status = "failed";
      showToast(`Extraction failed after retries: ${data.error || ""}`, "error");
      return "done";
    }
    await new Promise((r) => setTimeout(r, 2000));
    return "retry";
  }
  if (status === "failed") {
    item.status = "failed";
    showToast(`Extraction failed: ${data.error || ""}`, "error");
    return "done";
  }
  if (data.busy || status === "processing" || status === "saving") {
    // Another invocation (or a stale lease) owns the item — poll until free.
    item.pollCount = (item.pollCount || 0) + 1;
    if (item.pollCount > 10) {
      item.status = status;
      showToast(`Queue item still in progress: ${item.imageId}`, "info");
      return "done";
    }
    await new Promise((r) => setTimeout(r, 3000));
    return "retry";
  }
  // saved/reviewed/rejected or unknown — nothing to do.
  item.status = status;
  return "done";
}

async function openQueueReview(item, extracted) {
  let objectUrl = "";
  if (item.file) {
    objectUrl = URL.createObjectURL(item.file);
  } else {
    try {
      objectUrl = await getDownloadURL(ref(storage, item.storagePath));
    } catch (_) {}
  }
  reviewSession = {
    storagePaths: [item.storagePath],
    objectUrls: objectUrl ? [objectUrl] : [],
    currentPageIndex: 0,
    fileType: item.storagePath.endsWith(".pdf") ? "pdf" : "image",
    extracted,
    queueId: item.imageId,
    pHash: item.pHash || null,
  };
  openReviewPanel(extracted);
}

// On page load, pick up any queue item that never reached a terminal state and
// show it in the queue list on Home. Unlike a live upload batch, this does NOT
// auto-process or auto-open review modals — the user taps Review/Discard per
// item explicitly (persisted import queue = crash safety without modal ambush).
async function resumeImportQueue() {
  if (!currentUser || !currentPharmacyId) return;
  try {
    console.log("[resumeImportQueue] Checking for pending items in", currentPharmacyId);
    const q = query(
      collection(db, "pharmacies", currentPharmacyId, "importQueue"),
      where("status", "in", ["uploaded", "processing", "extracted", "ingested-partial"])
    );
    const snap = await getDocs(q);
    console.log("[resumeImportQueue] Found", snap.docs.length, "pending queue items");
    const items = snap.docs.map((d) => {
      const dta = d.data();
      return {
        imageId: d.id,
        storagePath: dta.storagePath || "",
        fileName: dta.fileName || d.id,
        status: dta.status || "uploaded",
        retries: 0,
        pollCount: 0,
        pdfPageNumber: dta.pdfPageNumber || null,
        pdfTotalPages: dta.pdfTotalPages || null,
        extracted: dta.extracted || null,
        pHash: dta.pHash || null,
        uploadedAt: dta.createdAt || null,
      };
    });
    if (items.length > 0) {
      importQueue = items;
      importQueueCursor = 0;
      importQueueAuto = false;
      renderImportQueueStatus();
      showToast(`${items.length} pending import(s) in queue — preparing for review.`, "info");
      // Auto-extract anything not yet "extracted", then open the first ready
      // review so the owner reviews images one by one with no waiting.
      startImportQueue();
    }
  } catch (err) {
    console.error("[resumeImportQueue] ERROR:", err.code, err.message, err);
  }
}

// Human-readable upload time for a queue item. Accepts a Firestore Timestamp
// (from a queue doc createdAt), a JS Date, epoch ms, or an ISO string; returns
// "" when unknown so the caller can hide the line entirely.
function formatUploadedAt(value) {
  if (value == null) return "";
  let d = null;
  if (typeof value.toDate === "function") d = value.toDate();
  else if (value instanceof Date) d = value;
  else if (typeof value === "number") d = new Date(value);
  else if (typeof value === "string") d = new Date(value);
  if (!d || isNaN(d.getTime())) return "";
  const now = new Date();
  const diffMin = Math.max(0, Math.floor((now - d.getTime()) / 60000));
  const rel = diffMin < 1 ? "just now" : diffMin < 60 ? `${diffMin}m ago` : diffMin < 1440 ? `${Math.floor(diffMin / 60)}h ago` : `${Math.floor(diffMin / 1440)}d ago`;
  const time = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  const sameDay = d.toDateString() === now.toDateString();
  return `Uploaded ${time}${sameDay ? "" : " " + d.toLocaleDateString()} · ${rel}`;
}

function renderImportQueueStatus() {
  const panel = $("import-queue-panel");
  if (!panel) return;
  const list = $("import-queue-list");
  if (!list) return;

  if (importQueue.length === 0) {
    panel.classList.add("hidden");
    return;
  }
  panel.classList.remove("hidden");
  const countEl = $("import-queue-count");
  if (countEl) countEl.textContent = `${importQueue.length} item(s) · ${importQueue.filter((i) => i.status === "extracted").length} ready for review`;

  list.innerHTML = importQueue.map((item, idx) => {
    const current = idx === importQueueCursor;
    const qs = item.status || "uploaded";
    const label = {
      uploaded: "Queued",
      processing: "Processing…",
      saving: "Saving…",
      extracted: "Ready for review",
      ingested: "Saved (merged)",
      "ingested-partial": "Staged",
      saved: "Saved",
      reviewed: "Reviewed",
      failed: "Failed",
      rejected: "Rejected",
    }[qs] || qs;
    const badgeClass = {
      failed: "bg-rose-500/15 text-rose-400",
      ingested: "bg-emerald-500/15 text-emerald-400",
      saved: "bg-emerald-500/15 text-emerald-400",
      extracted: "bg-amber-500/15 text-amber-400",
      uploaded: "bg-indigo-500/15 text-indigo-400",
      processing: "bg-indigo-500/15 text-indigo-400 animate-pulse",
      saving: "bg-indigo-500/15 text-indigo-400 animate-pulse",
      rejected: "bg-slate-500/15 text-slate-400",
      reviewed: "bg-slate-500/15 text-slate-400",
      "ingested-partial": "bg-amber-500/15 text-amber-400",
    }[qs] || "bg-slate-500/15 text-slate-400";

    // Review opens (or re-extracts) the item; Discard permanently deletes it.
    const showReview = ["uploaded", "processing", "saving", "extracted"].includes(qs);
    const showDiscard = !["saved", "reviewed", "ingested", "rejected"].includes(qs);
    let actions = "";
    if (showReview || showDiscard) {
      actions = `<div class="flex gap-1.5 shrink-0">
        ${showReview ? `<button onclick="window.reviewQueueItem('${item.imageId}')" class="text-[9px] px-2 py-1 bg-indigo-500/15 border border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/30 rounded-md font-bold transition-all">Review</button>` : ""}
        ${showDiscard ? `<button onclick="window.discardQueueItem('${item.imageId}')" class="text-[9px] px-2 py-1 bg-slate-800 border border-slate-700 text-slate-400 hover:text-rose-400 hover:border-rose-500/30 rounded-md font-bold transition-all">Discard</button>` : ""}
      </div>`;
    }

    return `<div class="flex justify-between items-center gap-2 px-3 py-1.5 rounded-lg ${
      current ? "bg-slate-800/80 border border-indigo-500/40" : "bg-slate-900/60 border border-slate-800"
    }">
      <div class="min-w-0">
        <p class="text-[10px] font-mono text-slate-300 truncate">${esc(item.fileName || item.imageId)}</p>
        <p class="text-[9px] text-slate-500 truncate">${esc(item.storagePath)}</p>
        ${item.uploadedAt ? `<p class="text-[9px] text-slate-500 truncate">${esc(formatUploadedAt(item.uploadedAt))}</p>` : ""}
      </div>
      <span class="text-[9px] px-1.5 py-0.5 rounded font-bold shrink-0 ${badgeClass}">${label}</span>
      ${actions}
    </div>`;
  }).join("");
}

// Explicit per-item Review action (queue list). Extracted items open the review
// panel directly; queued/processing items are run through the extraction
// pipeline first, then the review panel opens when they're ready.
window.reviewQueueItem = async (imageId) => {
  const item = importQueue.find((i) => i.imageId === imageId);
  if (!item) return;

  if (item.status === "extracted") {
    let extracted = item.extracted;
    if (!extracted) {
      try {
        const qSnap = await getDoc(queueItemRef(imageId));
        if (qSnap.exists) {
          extracted = qSnap.data().extracted || null;
          item.extracted = extracted;
        }
      } catch (_) {}
    }
    if (!extracted) {
      showToast("No extracted data for this item yet.", "error");
      return;
    }
    await openQueueReview(item, extracted);
    return;
  }

  if (["uploaded", "processing", "saving"].includes(item.status)) {
    // No full-screen blocker — extraction runs in the background while the
    // queue item shows "Processing…". The review panel opens when it's ready;
    // the owner can refresh or navigate freely (the queue is persisted).
    showToast("Extracting invoice…", "info");
    try {
      let outcome = "retry";
      let attempts = 0;
      while (outcome === "retry" && attempts < 20) {
        outcome = await processQueueItem(item);
        attempts++;
        renderImportQueueStatus();
      }
      if (outcome === "done" && !["ingested", "ingested-partial", "failed"].includes(item.status)) {
        showToast(`Item is ${item.status}.`, "info");
      }
    } catch (err) {
      console.error("[reviewQueueItem] failed:", err);
      showToast("Processing failed: " + err.message, "error");
    }
    return;
  }

  showToast(`Nothing to review — item is ${item.status}.`, "info");
};

// Explicit per-item Discard action (queue list). Deletes the queue doc and the
// raw Storage image via the discardQueueItem callable.
window.discardQueueItem = async (imageId, opts = {}) => {
  const item = importQueue.find((i) => i.imageId === imageId) || null;
  const label = item ? (item.fileName || item.imageId) : imageId;
  if (!opts.silent && !confirm(`Discard "${label}"? The uploaded image will be permanently deleted.`)) return;

  showLoadingOverlay(true, "Discarding…");
  try {
    const fn = httpsCallable(functions, "discardQueueItem", { timeout: 30000 });
    await fn({ pharmacyId: currentPharmacyId, docId: imageId });
    importQueue = importQueue.filter((i) => i.imageId !== imageId);
    if (importQueueCursor >= importQueue.length) {
      importQueueCursor = Math.max(0, importQueue.length - 1);
    }
    renderImportQueueStatus();
    if (!opts.silent) showToast("Discarded.", "warning");
  } catch (err) {
    console.error("[discardQueueItem] failed:", err.code, err.message, err);
    showToast("Discard failed: " + (err.message || err), "error");
  } finally {
    showLoadingOverlay(false);
  }
};

function updateReviewVisualSource() {
  const imgEl = $("review-invoice-img");
  const pdfEl = $("review-invoice-pdf");
  const url = reviewSession.objectUrls[reviewSession.currentPageIndex];

  if (url && (url.includes("pdf") || url.startsWith("blob:") && reviewSession.fileType === "pdf")) {
    imgEl.classList.add("hidden");
    pdfEl.classList.remove("hidden");
    pdfEl.src = url;
  } else {
    pdfEl.classList.add("hidden");
    imgEl.classList.remove("hidden");
    imgEl.src = url;
  }
}

function updateReviewPaginationUI() {
  const prevBtn = $("btn-prev-page");
  const nextBtn = $("btn-next-page");
  const txt = $("txt-page-num");
  const total = reviewSession.objectUrls.length;
  const current = reviewSession.currentPageIndex + 1;

  txt.textContent = `Page ${current}/${total}`;
  prevBtn.disabled = reviewSession.currentPageIndex === 0;
  nextBtn.disabled = reviewSession.currentPageIndex === total - 1;
}

async function splitPdfToImages(file) {
  const arrayBuffer = await file.arrayBuffer();
  const pdf = await pdfjsLib.getDocument({ data: arrayBuffer }).promise;
  const images = [];

  for (let pageNum = 1; pageNum <= pdf.numPages; pageNum++) {
    const page = await pdf.getPage(pageNum);
    const viewport = page.getViewport({ scale: 1.5 });
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');
    canvas.width = viewport.width;
    canvas.height = viewport.height;

    await page.render({ canvasContext: ctx, viewport: viewport }).promise();

    const blob = await new Promise(resolve => canvas.toBlob(resolve, 'image/jpeg', 0.9));
    const pageFile = new File([blob], `${file.name.replace(/\.[^/.]+$/, "")}_page_${pageNum}.jpg`, { type: 'image/jpeg' });
    images.push({ file: pageFile, pageNumber: pageNum, totalPages: pdf.numPages });
  }
  return images;
}

// ─── Perceptual Hash (dHash) for Duplicate Detection ───────────────────────────
// Difference hash: resize to 9x8 grayscale, compare adjacent pixels.
// Returns a 64-bit hash as hex string. Low Hamming distance = visually similar.

async function computeDHash(file) {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => {
      const size = 9; // 9x8 = 72 pixels, 64 bits of differences
      const canvas = document.createElement('canvas');
      canvas.width = size;
      canvas.height = size - 1; // 8
      const ctx = canvas.getContext('2d');
      ctx.drawImage(img, 0, 0, size, size - 1);
      const data = ctx.getImageData(0, 0, size, size - 1).data;
      let hash = 0;
      for (let y = 0; y < size - 1; y++) {
        for (let x = 0; x < size - 1; x++) {
          const i = (y * size + x) * 4;
          const j = (y * size + x + 1) * 4;
          // Grayscale luminance
          const left = 0.299 * data[i] + 0.587 * data[i + 1] + 0.114 * data[i + 2];
          const right = 0.299 * data[j] + 0.587 * data[j + 1] + 0.114 * data[j + 2];
          hash = (hash << 1) | (left > right ? 1 : 0);
        }
      }
      resolve(hash.toString(16).padStart(16, '0'));
    };
    img.onerror = reject;
    img.src = URL.createObjectURL(file);
  });
}

function hammingDistance(hexA, hexB) {
  const a = BigInt('0x' + hexA);
  const b = BigInt('0x' + hexB);
  let diff = a ^ b;
  let dist = 0;
  while (diff > 0) {
    dist += Number(diff & 1n);
    diff >>= 1n;
  }
  return dist;
}

// Fetch recent invoice pHashes for this pharmacy (last 60 days)
async function fetchRecentInvoiceHashes(pharmacyId) {
  const cutoff = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000);
  const q = query(
    collection(db, "pharmacies", pharmacyId, "invoices"),
    where("createdAt", ">=", cutoff),
    orderBy("createdAt", "desc"),
    limit(200)
  );
  const snap = await getDocs(q);
  const hashes = [];
  snap.forEach((doc) => {
    const d = doc.data();
    if (d.pHash) {
      hashes.push({ pHash: d.pHash, invoiceNumber: d.invoiceNumber, distributor: d.distributor, createdAt: d.createdAt });
    }
  });
  return hashes;
}

// Check if a new image is a near-duplicate of any recent invoice
// Returns { isDuplicate: boolean, match: { pHash, invoiceNumber, distributor, createdAt, distance } | null }
async function checkDuplicateByHash(file, pharmacyId) {
  const hash = await computeDHash(file);
  const recent = await fetchRecentInvoiceHashes(pharmacyId);
  const THRESHOLD = 8; // Hamming distance ≤ 8 = likely same invoice (tune as needed)
  for (const h of recent) {
    const dist = hammingDistance(hash, h.pHash);
    if (dist <= THRESHOLD) {
      return { isDuplicate: true, match: { ...h, distance: dist, newHash: hash } };
    }
  }
  return { isDuplicate: false, match: null, newHash: hash };
}

// Layer 2: Compound key duplicate check has MOVED server-side — saveInvoice now
// hard-blocks on (distributorId, invoiceNumber) before writing, so it cannot be
// bypassed by client bugs. The pHash check above (Layer 1) remains as the only
// client-side duplicate guard, purely as an upload-time UX warning.

// ─── Purchase Summary (Reports tab) ───────────────────────────────────────────
// Lightweight monthly Purchase Summary. Builds ONLY on the invoiceSummary fields
// already persisted by saveInvoice — no new schema, no GSTR-3B. Aggregation is
// done server-side by the purchaseSummary callable; this view renders the
// distributor → tax-slab breakdown and offers a CA-ready CSV export.

let lastPurchaseSummary = null; // raw response from purchaseSummary, for CSV export

function initPurchaseReports() {
  const monthInput = $("report-month");
  if (!monthInput) return;
  const now = new Date();
  monthInput.value = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
  $("btn-report-load").addEventListener("click", loadPurchaseReport);
  $("btn-report-export").addEventListener("click", exportPurchaseCSV);
}

function monthRange(ym) {
  const [y, m] = String(ym).split("-").map((n) => Number(n));
  if (!y || !m) return null;
  const from = new Date(Date.UTC(y, m - 1, 1));
  const to = new Date(Date.UTC(y, m, 1));
  return { from: from.toISOString(), to: to.toISOString() };
}

async function loadPurchaseReport() {
  const monthInput = $("report-month");
  const range = monthRange(monthInput.value || "");
  if (!range) {
    showToast("Pick a valid month first.", "warning");
    return;
  }
  if (!currentUser) {
    showToast("Not signed in.", "error");
    return;
  }
  const btn = $("btn-report-load");
  btn.disabled = true;
  btn.textContent = "Loading…";
  $("report-body").innerHTML = `<div class="text-center py-8 text-xs text-slate-500">Loading summary…</div>`;
  try {
    const fn = httpsCallable(functions, "purchaseSummary", { timeout: 60000 });
    const resp = await fn({ pharmacyId: currentPharmacyId, from: range.from, to: range.to });
    lastPurchaseSummary = resp.data;
    renderPurchaseSummary(resp.data);
  } catch (err) {
    console.error("[purchaseSummary] ERROR:", err);
    $("report-body").innerHTML = `<div class="text-center py-8 text-xs text-rose-400">Failed to load: ${err.message}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = "Load";
  }
}

const fmtINR = (n) => "₹" + Number(n || 0).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

function renderPurchaseSummary(data) {
  const g = data.grandTotals || {};
  $("report-total-purchases").textContent = fmtINR(g.taxable || 0);
  $("report-total-gst").textContent = fmtINR(g.gst || 0);
  $("report-total-invoices").textContent = String(g.invoiceCount || 0);

  const dists = data.distributors || [];
  const body = $("report-body");
  if (dists.length === 0) {
    body.innerHTML = `<div class="text-center py-8 text-xs text-slate-500">No saved invoices in this month.</div>`;
    return;
  }

  body.innerHTML = dists.map((d) => {
    const slabsRows = (d.slabs || []).map((s) => `
      <tr class="border-t border-slate-800/60">
        <td class="px-3 py-1.5 text-[10px] text-slate-400 pl-8">${s.rate > 0 ? s.rate + "%" : "0% / Exempt"}</td>
        <td class="px-3 py-1.5 text-[10px] text-right font-mono text-slate-200">${fmtINR(s.taxable)}</td>
        <td class="px-3 py-1.5 text-[10px] text-right font-mono text-slate-400">${fmtINR(s.cgst)}</td>
        <td class="px-3 py-1.5 text-[10px] text-right font-mono text-slate-400">${fmtINR(s.sgst)}</td>
        <td class="px-3 py-1.5 text-[10px] text-right font-mono text-slate-400">${fmtINR(s.igst)}</td>
        <td class="px-3 py-1.5 text-[10px] text-right font-mono text-indigo-400 font-bold">${fmtINR(s.gst)}</td>
      </tr>`).join("");
    const totalRow = `
      <tr class="border-t border-slate-700">
        <td class="px-3 py-1.5 text-[10px] font-bold text-slate-200 pl-8">Total</td>
        <td class="px-3 py-1.5 text-[10px] text-right font-mono font-bold text-slate-100">${fmtINR(d.taxable)}</td>
        <td class="px-3 py-1.5 text-[10px] text-right font-mono text-slate-300">${fmtINR(d.cgst)}</td>
        <td class="px-3 py-1.5 text-[10px] text-right font-mono text-slate-300">${fmtINR(d.sgst)}</td>
        <td class="px-3 py-1.5 text-[10px] text-right font-mono text-slate-300">${fmtINR(d.igst)}</td>
        <td class="px-3 py-1.5 text-[10px] text-right font-mono font-bold text-indigo-300">${fmtINR(d.gst)}</td>
      </tr>`;
    return `
      <div class="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden">
        <div class="px-4 py-2.5 flex items-center justify-between bg-slate-800/40">
          <span class="text-xs font-heading font-bold text-slate-100">${escapeHtml(d.name)}</span>
          <span class="text-[9px] text-slate-400 font-mono">${d.invoiceCount} invoice(s) · Grand total ${fmtINR(d.grandTotal)}</span>
        </div>
        <div class="overflow-x-auto">
          <table class="w-full text-left">
            <thead>
              <tr class="text-[9px] uppercase tracking-wider text-slate-500">
                <th class="px-3 py-1.5 pl-8">Slab</th>
                <th class="px-3 py-1.5 text-right">Taxable</th>
                <th class="px-3 py-1.5 text-right">CGST</th>
                <th class="px-3 py-1.5 text-right">SGST</th>
                <th class="px-3 py-1.5 text-right">IGST</th>
                <th class="px-3 py-1.5 text-right">GST Total</th>
              </tr>
            </thead>
            <tbody>${slabsRows}${totalRow}</tbody>
          </table>
        </div>
      </div>`;
  }).join("");
}

// Escape a string for safe injection into innerHTML (distributor names come from
// OCR, so they must never break out of the rendered markup).
function escapeHtml(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function exportPurchaseCSV() {
  if (!lastPurchaseSummary) {
    showToast("Load a month first.", "warning");
    return;
  }
  const data = lastPurchaseSummary;
  const period = data.period || {};
  const monthLabel = (period.from || "").slice(0, 7);

  const rows = [];
  rows.push(["Pharmacy Purchase Summary - " + monthLabel]);
  rows.push([""]);
  rows.push(["Distributor", "Slab %", "Taxable Rs", "CGST Rs", "SGST Rs", "IGST Rs", "Total GST Rs"]);
  const g = data.grandTotals || {};
  for (const d of data.distributors || []) {
    for (const s of d.slabs || []) {
      rows.push([
        d.name,
        s.rate > 0 ? s.rate : "Exempt",
        numCSV(s.taxable), numCSV(s.cgst), numCSV(s.sgst), numCSV(s.igst), numCSV(s.gst),
      ]);
    }
    rows.push([
      d.name + " - TOTAL", "",
      numCSV(d.taxable), numCSV(d.cgst), numCSV(d.sgst), numCSV(d.igst), numCSV(d.gst),
    ]);
  }
  rows.push([
    "GRAND TOTAL", "",
    numCSV(g.taxable), numCSV(g.cgst), numCSV(g.sgst), numCSV(g.igst), numCSV(g.gst),
  ]);
  rows.push([""]);
  rows.push(["Invoices in month", String(g.invoiceCount || 0)]);

  const csv = rows.map((r) => r.map((cell) => `"${String(cell == null ? "" : cell).replace(/"/g, '""')}"`).join(",")).join("\r\n");
  const blob = new Blob(["\ufeff" + csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `purchase-summary-${monthLabel}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
  showToast("CSV exported.", "success");
}

function numCSV(n) {
  return Number(n || 0).toFixed(2);
}
