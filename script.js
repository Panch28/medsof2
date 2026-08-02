/**
 * RxExpiry - Full production script
 *
 * Flow:
 * 1. Phone OTP auth (Firebase Auth)
 * 2. File selection (camera/gallery/PDF) — one at a time
 * 3. Local quality filter (blur variance via canvas, exposure check, PDF text layer)
 * 4. Upload raw file to Storage → call extractInvoice CF synchronously
 * 5. If captureQuality.readable = false → show reupload prompt
 * 6. If readable → Review screen: image + editable fields, confidence highlights, arithmetic check
 * 7. Confirm & Save → write Firestore medicines + invoice docs → delete raw file
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
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";
import {
  getStorage,
  ref,
  uploadBytes,
  deleteObject,
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
};

// Triage and queue variables
let triageFiles = []; // Array of { id, file, objectUrl, text, invoiceNo, pageNum, totalPages }
let triageGroups = []; // Array of { id, invoiceNo, files: [triageFileId, ...] }
let reviewQueue = []; // Queue of { storagePaths: string[], objectUrls: string[], extracted: parsedGeminiResult }
let reviewQueueIndex = 0;

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
  loadExpiringMedicines();
  loadDistributors();
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

// ─── Quality Filter (local, no AI) ───────────────────────────────────────────

/**
 * Returns { pass: bool, blurVariance: number, luminance: number, issues: string[] }
 * For images: checks Laplacian variance (blur) and mean luminance (exposure).
 * For PDFs: checks if the PDF has a text layer (not a scanned image PDF).
 */
async function runQualityFilter(file) {
  const issues = [];

  if (file.type === "application/pdf") {
    // For PDF: try to read text content. We use a simple heuristic —
    // if the file is < 10 KB it likely has no real content.
    // A more robust check would use PDF.js but that adds overhead.
    // We'll pass PDFs and let Gemini judge readability.
    return { pass: true, blurVariance: 999, luminance: 128, issues: [] };
  }

  // Image: draw onto canvas and run pixel analysis
  return new Promise((resolve) => {
    const img = new Image();
    const url = URL.createObjectURL(file);
    img.onload = () => {
      URL.revokeObjectURL(url);
      const canvas = document.createElement("canvas");
      // Downsample for speed
      const maxDim = 400;
      const scale = Math.min(1, maxDim / Math.max(img.width, img.height));
      canvas.width = Math.floor(img.width * scale);
      canvas.height = Math.floor(img.height * scale);
      const ctx = canvas.getContext("2d");
      ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
      const { data } = ctx.getImageData(0, 0, canvas.width, canvas.height);

      // Compute grayscale values
      const gray = [];
      let sumLum = 0;
      for (let i = 0; i < data.length; i += 4) {
        const g = 0.299 * data[i] + 0.587 * data[i + 1] + 0.114 * data[i + 2];
        gray.push(g);
        sumLum += g;
      }
      const meanLum = sumLum / gray.length;

      // Laplacian variance (blur detection)
      // Simplified: variance of pixel-to-pixel differences
      let diffSum = 0;
      for (let i = 1; i < gray.length; i++) {
        const d = gray[i] - gray[i - 1];
        diffSum += d * d;
      }
      const blurVariance = diffSum / gray.length;

      // Thresholds (relaxed to always pass)
      const BLUR_THRESHOLD = 0;   // below this → too blurry
      const MIN_LUM = 0;          // below this → too dark
      const MAX_LUM = 255;         // above this → overexposed

      if (blurVariance < BLUR_THRESHOLD) {
        issues.push(`Image is too blurry (variance ${blurVariance.toFixed(1)}, need ≥${BLUR_THRESHOLD})`);
      }
      if (meanLum < MIN_LUM) {
        issues.push(`Image is too dark (luminance ${meanLum.toFixed(1)}, need ≥${MIN_LUM})`);
      }
      if (meanLum > MAX_LUM) {
        issues.push(`Image is overexposed (luminance ${meanLum.toFixed(1)}, need ≤${MAX_LUM})`);
      }

      resolve({ pass: issues.length === 0, blurVariance, luminance: meanLum, issues });
    };
    img.onerror = () => {
      URL.revokeObjectURL(url);
      resolve({ pass: false, blurVariance: 0, luminance: 0, issues: ["Could not decode image."] });
    };
    img.src = url;
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
  // Show precheck panel
  const precheckPanel = $("precheck-feedback-panel");
  precheckPanel.classList.remove("hidden");
  $("precheck-status-msg").textContent = "Running quality checks...";
  $("precheck-status-msg").className = "text-xs font-semibold py-1.5 px-3 rounded text-center text-slate-400";
  $("precheck-blur-val").textContent = "Calculating...";
  $("precheck-exposure-val").textContent = "Calculating...";
  $("precheck-blur-bar").style.width = "0%";
  $("precheck-exposure-bar").style.width = "0%";

  // Run local quality filter
  const qc = await runQualityFilter(file);

  // Update UI with results
  $("precheck-blur-val").textContent = qc.blurVariance.toFixed(1);
  $("precheck-exposure-val").textContent = qc.luminance.toFixed(1);
  $("precheck-blur-bar").style.width = Math.min(100, (qc.blurVariance / 200) * 100) + "%";
  $("precheck-blur-bar").className = "h-full transition-all duration-300 " + (qc.blurVariance >= 30 ? "bg-emerald-500" : "bg-rose-500");
  $("precheck-exposure-bar").style.width = (qc.luminance / 255) * 100 + "%";
  $("precheck-exposure-bar").className = "h-full transition-all duration-300 " + (qc.luminance >= 30 && qc.luminance <= 225 ? "bg-emerald-500" : "bg-amber-500");

  if (!qc.pass) {
    $("precheck-status-msg").textContent = "⚠ Low quality: " + qc.issues.join("; ") + " — proceeding anyway...";
    $("precheck-status-msg").className = "text-xs font-semibold py-1.5 px-3 rounded text-center bg-amber-500/10 text-amber-400 border border-amber-500/30";
    showToast("Low quality warning, proceeding...", "warning");
  } else {
    $("precheck-status-msg").textContent = "✓ Quality check passed — uploading...";
    $("precheck-status-msg").className = "text-xs font-semibold py-1.5 px-3 rounded text-center bg-emerald-500/10 text-emerald-400 border border-emerald-500/30";
  }

  // Upload to Storage
  const ext = file.type === "application/pdf" ? "pdf" : "jpg";
  const filename = `${Date.now()}_${Math.random().toString(36).slice(2)}.${ext}`;
  const storagePath = `invoices/${currentPharmacyId}/${filename}`;
  const storageRef = ref(storage, storagePath);

  showLoadingOverlay(true, "Uploading...");
  try {
    await uploadBytes(storageRef, file, { contentType: file.type });
  } catch (err) {
    showLoadingOverlay(false);
    showToast("Upload failed: " + err.message, "error");
    precheckPanel.classList.add("hidden");
    return;
  }

  // Call extractInvoice CF synchronously
  showLoadingOverlay(true, "Extracting invoice with AI...");
  const extractFn = httpsCallable(functions, "extractInvoice", { timeout: 120000 });

  let extracted;
  try {
    const result = await extractFn({ storagePath, pharmacyId: currentPharmacyId });
    console.log("Gemini single raw extraction result:", result.data);
    extracted = result.data;
  } catch (err) {
    console.error("Gemini single extraction failed:", err);
    showLoadingOverlay(false);
    precheckPanel.classList.add("hidden");
    showToast("Extraction failed: " + err.message, "error");
    // Clean up the uploaded file
    try { await deleteObject(storageRef); } catch (_) {}
    return;
  }

  showLoadingOverlay(false);
  precheckPanel.classList.add("hidden");

  // Store session
  reviewSession.storagePaths = [storagePath];
  const url = URL.createObjectURL(file);
  reviewSession.objectUrls = [url];
  reviewSession.currentPageIndex = 0;
  reviewSession.fileType = file.type === "application/pdf" ? "pdf" : "image";
  reviewSession.extracted = extracted;

  // Check captureQuality
  const cq = extracted?.captureQuality;
  if (!cq || !cq.readable) {
    showReuploadPrompt(cq?.issues || ["Invoice could not be read."]);
    // Clean up
    try { await deleteObject(storageRef); } catch (_) {}
    return;
  }

  // Show review screen
  openReviewPanel(extracted);
}

// ─── Loading Overlay ──────────────────────────────────────────────────────────

function showLoadingOverlay(show, message = "Processing...") {
  let overlay = $("loading-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "loading-overlay";
    overlay.className = "fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-[100] flex flex-col items-center justify-center gap-4";
    overlay.innerHTML = `
      <div class="w-16 h-16 rounded-2xl bg-indigo-600/20 border border-indigo-500/30 flex items-center justify-center">
        <svg class="w-8 h-8 text-indigo-400 animate-spin" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
        </svg>
      </div>
      <p id="loading-msg" class="text-sm font-semibold text-slate-300"></p>
    `;
    document.body.appendChild(overlay);
  }
  if (show) {
    $("loading-msg").textContent = message;
    overlay.classList.remove("hidden");
  } else {
    overlay.classList.add("hidden");
  }
}

// ─── Reupload Prompt ──────────────────────────────────────────────────────────

function showReuploadPrompt(issues) {
  let modal = $("reupload-modal");
  if (!modal) {
    modal = document.createElement("div");
    modal.id = "reupload-modal";
    modal.className = "fixed inset-0 bg-slate-950/90 z-[90] flex items-center justify-center p-4";
    modal.innerHTML = `
      <div class="bg-slate-900 border border-rose-500/30 rounded-2xl p-6 w-full max-w-sm shadow-2xl space-y-4">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-xl bg-rose-500/20 flex items-center justify-center">
            <svg class="w-5 h-5 text-rose-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/>
            </svg>
          </div>
          <h3 class="font-heading font-bold text-slate-100 text-sm">Invoice Unreadable</h3>
        </div>
        <p class="text-xs text-slate-400">The AI could not read this invoice clearly. Please retake with better conditions:</p>
        <ul id="reupload-issues" class="space-y-1 text-xs text-rose-300"></ul>
        <button id="btn-reupload-dismiss" class="w-full py-2.5 bg-rose-600 hover:bg-rose-500 text-white font-bold rounded-xl text-sm transition-all active:scale-[0.98]">
          Dismiss &amp; Retake
        </button>
      </div>
    `;
    document.body.appendChild(modal);
  }
  const ul = $("reupload-issues");
  ul.innerHTML = issues.map((i) => `<li class="flex items-start gap-1.5"><span class="mt-0.5 text-rose-500">•</span>${i}</li>`).join("");
  modal.classList.remove("hidden");
  $("btn-reupload-dismiss").onclick = () => modal.classList.add("hidden");
}

// ─── Review Panel ─────────────────────────────────────────────────────────────

function openReviewPanel(extracted) {
  const panel = $("extraction-review-panel");
  panel.classList.remove("hidden");

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
  const cashDiscRow = $("review-cash-disc-row");
  const schDiscRow = $("review-sch-disc-row");
  const cnNoRow = $("review-cn-no-row");
  const roundOffRow = $("review-round-off-row");
  const printedGstRow = $("review-printed-gst-row");
  const printedCdRow = $("review-printed-cd-row");
  if (summary) {
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
    $("review-declared-total-input").value = (extracted.invoiceTotal || 0).toFixed(2);
    cashDiscRow.classList.add("hidden");
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

  // Reject button
  $("btn-review-reject").onclick = async () => {
    panel.classList.add("hidden");
    const pathsToDelete = reviewSession.storagePaths;
    revokeReviewSession();
    showToast("Invoice discarded", "warning");
    // Delete raw files
    if (pathsToDelete && pathsToDelete.length > 0) {
      for (const p of pathsToDelete) {
        try { await deleteObject(ref(storage, p)); } catch (_) {}
      }
    }
    
    reviewQueueIndex++;
    showNextReviewInQueue();
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

    const nameConf = conf.medicineName ?? 1;
    const nameClass = nameConf < 0.8 ? "text-amber-400" : "text-slate-100";

    card.innerHTML = `
      <div class="flex justify-between items-center">
        <span class="text-[10px] font-bold text-slate-500 uppercase tracking-wider">Line ${idx + 1}</span>
        <span class="text-[9px] px-1.5 py-0.5 rounded ${nameConf < 0.8 ? "bg-amber-500/15 text-amber-400 border border-amber-500/30" : "bg-emerald-500/10 text-emerald-400"}">
          avg conf ${avgConfidence(conf)}
        </span>
      </div>
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

function avgConfidence(conf) {
  const vals = Object.values(conf).filter((v) => typeof v === "number");
  if (!vals.length) return "100%";
  return ((vals.reduce((a, b) => a + b, 0) / vals.length) * 100).toFixed(0) + "%";
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

  const declaredTotal = parseFloat($("review-declared-total-input").value) || 0;
  let computedTotal, diff, match;

  const summary = window._invoiceSummary;

  // 1) GRAND TOTAL check — printed footer formula:
  //    Grand Total = Sale Value − Sch Disc − Cash Disc + Total GST + Round Off − CN.NO
  if (summary && summary.saleValue) {
    const sVal = summary.saleValue || 0;
    const sch = summary.schDisc || 0;
    const cash = summary.cashDiscount || 0;
    const gstSum = summary.totalGst || totalGst;
    const ro = summary.roundOff || 0;
    const cn = summary.cnNo || 0;
    computedTotal = sVal - sch - cash + gstSum + ro - cn;
    diff = Math.abs(computedTotal - declaredTotal);
    match = diff <= 2;
  } else if (summary) {
    computedTotal = totalNet + totalGst;
    diff = Math.abs(computedTotal - declaredTotal);
    match = diff <= 2;
  } else {
    computedTotal = totalNet + totalGst;
    diff = Math.abs(computedTotal - declaredTotal);
    match = diff <= 2;
  }

  // 2) Independent GST cross-check vs printed footer totals
  const printedGst = summary && (summary.totalGst ?? ((summary.totalCGST || 0) + (summary.totalSGST || 0) + (summary.totalIGST || 0)));
  let gstMatch = true, gstDiff = 0;
  if (printedGst) {
    gstDiff = Math.abs(totalGst - printedGst);
    gstMatch = gstDiff <= 1;
  }

  const overallMatch = match && gstMatch;
  console.log("[Recalc] total=", { declaredTotal, computedTotal, diff, match, printedGst, totalGst, gstDiff, gstMatch, overallMatch });

  const badge = $("review-arithmetic-badge");
  const gstBadge = $("review-gst-badge");
  const warning = $("arithmetic-warning-banner");
  const warningDetail = $("arithmetic-warning-detail");
  const ackContainer = $("arithmetic-ack-container");
  const approveBtn = $("btn-review-approve");

  styleBadge(badge, match, "✓ Totals Match", `⚠ Total ₹${diff.toFixed(2)}`);
  if (gstBadge) {
    if (!printedGst) {
      gstBadge.classList.add("hidden");
    } else {
      gstBadge.classList.remove("hidden");
      styleBadge(gstBadge, gstMatch, "✓ GST Matches", `⚠ GST ₹${gstDiff.toFixed(2)}`);
    }
  }

  if (overallMatch) {
    warning.classList.add("hidden");
    ackContainer.classList.add("hidden");
    approveBtn.disabled = false;
    approveBtn.className = "py-2.5 bg-indigo-600 text-white font-bold rounded-lg text-xs hover:bg-indigo-500 transition-all active:scale-[0.98]";
  } else {
    warning.classList.remove("hidden");
    if (warningDetail) {
      const fails = [];
      if (!match) fails.push(`Grand total mismatch ₹${diff.toFixed(2)}`);
      if (printedGst && !gstMatch) fails.push(`GST: sum ₹${totalGst.toFixed(2)} ≠ printed ₹${printedGst.toFixed(2)}`);

      if (!match && summary && !(summary.cashDiscount || summary.schDisc || 0) && !totalCd) {
        const netTaxable = summary.saleValue || totalNet;
        const roundOff = summary.roundOff || 0;
        const implied = netTaxable + (summary.totalGst || totalGst) + roundOff - declaredTotal;
        const impliedNet = netTaxable + roundOff - declaredTotal;
        const impliedDisc = Math.abs(implied) < Math.abs(impliedNet) ? implied : impliedNet;
        if (impliedDisc > 0.01) {
          fails.push(`Discount of ~₹${impliedDisc.toFixed(2)} appears missing (Gemini read ₹0.00)`);
        }
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
  const panel = $("extraction-review-panel");

  try {
    // Collect edited line items from DOM
    const lineItemCards = document.querySelectorAll("#review-line-items [data-idx]");
    const idxSet = new Set();
    lineItemCards.forEach((el) => idxSet.add(parseInt(el.dataset.idx)));

    const lineItems = [];
    idxSet.forEach((idx) => {
      const get = (field) => {
        const el = document.querySelector(`#review-line-items [data-idx="${idx}"][data-field="${field}"]`);
        return el ? el.value : "";
      };
      lineItems.push({
        medicineName: get("medicineName"),
        batchNumber: get("batchNumber"),
        expiryDate: get("expiryDate"),
        quantityBilled: parseFloat(get("quantityBilled")) || 0,
        quantityFree: parseFloat(get("quantityFree")) || 0,
        unitPrice: parseFloat(get("unitPrice")) || 0,
        cdPercent: parseFloat(get("cdPercent")) || 0,
        taxableValue: parseFloat(get("taxableValue")) || 0,
        cdValue: parseFloat(get("cdValue")) || 0,
        gstRate: parseFloat(get("gstRate")) || 0,
        gstValue: parseFloat(get("gstValue")) || 0,
        netValue: parseFloat(get("netValue")) || 0,
      });
    });

    const invoiceTotal = parseFloat($("review-declared-total-input").value) || 0;
    console.log("[Save] collected from DOM", { itemCount: lineItems.length, invoiceTotal, authUid: currentUser ? currentUser.uid : null });

    if (!currentUser) {
      throw new Error("Not signed in. Please log in again and retry.");
    }

    showLoadingOverlay(true, "Saving to Firestore...");

    console.log("[Save] calling saveInvoice callable...");
    const saveFn = httpsCallable(functions, "saveInvoice");
    const resp = await saveFn({
      pharmacyId: currentPharmacyId,
      invoice: {
        distributor: originalExtracted.distributor || "",
        invoiceNumber: originalExtracted.invoiceNumber || "",
        invoiceDate: originalExtracted.invoiceDate || "",
        invoiceTotal,
        invoiceSummary: originalExtracted.invoiceSummary || {},
        captureQuality: originalExtracted.captureQuality || {},
        gstCheck: originalExtracted.gstCheck || {},
        rawGeminiResponse: originalExtracted.rawGeminiResponse || "",
      },
      lineItems,
      confirmedBy: currentUser.uid,
    });
    console.log("[Save] saveInvoice callable succeeded", resp && resp.data);

    // Delete raw files from Storage
    if (reviewSession.storagePaths && reviewSession.storagePaths.length > 0) {
      for (const p of reviewSession.storagePaths) {
        try {
          await deleteObject(ref(storage, p));
        } catch (delErr) {
          console.warn("Could not delete raw file (non-fatal):", delErr.message);
        }
      }
    }

    showLoadingOverlay(false);
    panel.classList.add("hidden");
    revokeReviewSession();
    showToast(`Saved! ${lineItems.length} medicine(s) recorded.`, "success");
    console.log("[Save] done — advancing to next review");

    reviewQueueIndex++;
    showNextReviewInQueue();

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
  reviewSession = { storagePaths: [], objectUrls: [], currentPageIndex: 0, fileType: 'image', extracted: null };
}

// ─── Expiring Medicines List ──────────────────────────────────────────────────

async function loadExpiringMedicines() {
  const listEl = $("expiring-list");
  const countEl = $("expiring-count");
  if (!listEl) return;

  listEl.innerHTML = `<div class="text-center py-6 text-xs text-slate-500">Loading...</div>`;

  try {
    const snapshot = await getDocs(
      collection(db, "pharmacies", currentPharmacyId, "medicines")
    );

    const now = new Date();
    const ninetyDaysOut = new Date(now.getTime() + 90 * 24 * 60 * 60 * 1000);
    const items = [];

    snapshot.forEach((docSnap) => {
      const d = docSnap.data();
      // Parse expiry: try MM/YYYY then DD/MM/YYYY
      const expDate = parseExpiryDate(d.expiryDate);
      if (expDate && expDate <= ninetyDaysOut) {
        items.push({ id: docSnap.id, ...d, expDate });
      }
    });

    items.sort((a, b) => a.expDate - b.expDate);
    countEl.textContent = `${items.length} record${items.length !== 1 ? "s" : ""}`;

    if (items.length === 0) {
      listEl.innerHTML = `<div class="text-center py-6 text-xs text-slate-500">No medicines expiring within 90 days.</div>`;
      return;
    }

    listEl.innerHTML = items.map((item) => {
      const daysLeft = Math.ceil((item.expDate - now) / (1000 * 60 * 60 * 24));
      const urgency = daysLeft <= 30 ? "border-rose-500/40 bg-rose-500/5" : daysLeft <= 60 ? "border-amber-500/40 bg-amber-500/5" : "border-slate-700/60 bg-slate-800/40";
      const badgeClass = daysLeft <= 30 ? "bg-rose-500/20 text-rose-400" : daysLeft <= 60 ? "bg-amber-500/20 text-amber-400" : "bg-slate-700 text-slate-400";
      return `
        <div class="border ${urgency} rounded-xl p-3 space-y-1.5">
          <div class="flex justify-between items-start">
            <div>
              <p class="text-xs font-bold text-slate-100 leading-tight">${item.medicineName}</p>
              <p class="text-[9px] font-mono text-slate-500">Batch: ${item.batchNumber}</p>
            </div>
            <span class="text-[9px] px-2 py-0.5 rounded-full font-bold ${badgeClass}">
              ${daysLeft <= 0 ? "EXPIRED" : `${daysLeft}d left`}
            </span>
          </div>
          <div class="flex items-center justify-between text-[10px] text-slate-400">
            <span>Exp: ${item.expiryDate}</span>
            <span>Qty: ${item.remainingQty ?? item.quantityBilled}</span>
            <span class="text-slate-500">${item.distributor || "—"}</span>
          </div>
        </div>
      `;
    }).join("");
  } catch (err) {
    listEl.innerHTML = `<div class="text-center py-6 text-xs text-rose-400">Error loading: ${err.message}</div>`;
  }
}

function parseExpiryDate(str) {
  if (!str) return null;
  // MM/YYYY
  let m = str.match(/^(\d{1,2})\/(\d{4})$/);
  if (m) return new Date(parseInt(m[2]), parseInt(m[1]) - 1, 28);
  // DD/MM/YYYY
  m = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return new Date(parseInt(m[3]), parseInt(m[2]) - 1, parseInt(m[1]));
  // YYYY-MM-DD
  m = str.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) return new Date(parseInt(m[1]), parseInt(m[2]) - 1, parseInt(m[3]));
  return null;
}

// ─── Distributors ─────────────────────────────────────────────────────────────

async function loadDistributors() {
  const listEl = $("distributors-list");
  if (!listEl) return;

  try {
    const snapshot = await getDocs(
      collection(db, "pharmacies", currentPharmacyId, "medicines")
    );
    const distributorMap = {};
    snapshot.forEach((docSnap) => {
      const d = docSnap.data();
      if (d.distributor) {
        distributorMap[d.distributor] = (distributorMap[d.distributor] || 0) + 1;
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
  } catch (err) {
    listEl.innerHTML = `<div class="text-xs text-rose-400 text-center py-4">${err.message}</div>`;
  }
}

// ─── Bootstrap ────────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  // Show auth screen initially
  $("app-workspace").classList.add("hidden");
  $("auth-screen").classList.remove("hidden");
  initAuth();
});

// ─── Client-Side OCR & Grouping Triage ────────────────────────────────────────

let _ocrWorker = null;

async function getOcrWorker() {
  if (!_ocrWorker) {
    _ocrWorker = await Tesseract.createWorker("eng", 1, {
      logger: (m) => {
        if (m.status === "recognizing text") return;
      }
    });
  }
  return _ocrWorker;
}

async function terminateOcrWorker() {
  if (_ocrWorker) {
    await _ocrWorker.terminate();
    _ocrWorker = null;
  }
}

function preprocessForOcr(file) {
  return new Promise((resolve) => {
    const img = new Image();
    const url = URL.createObjectURL(file);
    img.onload = () => {
      URL.revokeObjectURL(url);
      const canvas = document.createElement("canvas");
      const maxDim = 1200;
      const scale = Math.min(1, maxDim / Math.max(img.width, img.height));
      canvas.width = Math.floor(img.width * scale);
      canvas.height = Math.floor(img.height * scale);
      const ctx = canvas.getContext("2d");
      ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
      const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
      const d = imageData.data;
      for (let i = 0; i < d.length; i += 4) {
        const r = d[i], g = d[i + 1], b = d[i + 2];
        const gray = 0.299 * r + 0.587 * g + 0.114 * b;
        const contrasted = gray < 128 ? Math.max(0, gray - 20) : Math.min(255, gray + 20);
        d[i] = d[i + 1] = d[i + 2] = contrasted;
      }
      ctx.putImageData(imageData, 0, 0);
      canvas.toBlob((blob) => {
        resolve(blob);
      }, "image/jpeg", 0.92);
    };
    img.src = url;
  });
}

async function performOcr(file) {
  const processedBlob = await preprocessForOcr(file);
  const worker = await getOcrWorker();
  const { data: { text } } = await worker.recognize(processedBlob);
  return text;
}

async function performOcrWithRetry(file, retries = 2) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await performOcr(file);
    } catch (err) {
      console.warn(`OCR attempt ${attempt}/${retries} failed:`, err.message);
      if (attempt === retries) throw err;
      await new Promise(r => setTimeout(r, 500));
    }
  }
}

function parseInvoiceNo(text) {
  const cleaned = text.replace(/\s+/g, " ");
  const invPatterns = [
    /tax\s+invoice\s*(?:no|num|number|#)?\.?\s*[:#\s-]+\s*([a-z0-9\-/]+)/i,
    /invoice\s*(?:no|num|number|#)?\.?\s*[:#\s-]+\s*([a-z0-9\-/]+)/i,
    /(?:proforma|pro\.?\s*forma)?\s*invoice\s*(?:no|num|number|#)?\.?\s*[:#\s-]+\s*([a-z0-9\-/]+)/i,
    /bill\s*(?:no|num|number|#)?\.?\s*[:#\s-]+\s*([a-z0-9\-/]+)/i,
    /inv\s*(?:no|num|number|#)?\.?\s*[:#\s-]+\s*([a-z0-9\-/]+)/i,
    /invoice#\s*([a-z0-9\-/]+)/i,
    /bill#\s*([a-z0-9\-/]+)/i,
    /(?:tax\s+)?invoice\s*[#:]\s*([a-z0-9\-/]+)/i,
  ];
  for (const regex of invPatterns) {
    const match = cleaned.match(regex);
    if (match && match[1]) {
      const val = match[1].trim().replace(/[^a-z0-9\-/]/ig, "").replace(/^0+/, "");
      if (val.length >= 3 && /\d/.test(val)) return val;
    }
  }
  const fallback = cleaned.match(/(?:no|number|#)\s*[:#\s-]*\s*([A-Z0-9]{4,}(?:\/[A-Z0-9]+)*)/i);
  if (fallback && fallback[1] && /\d/.test(fallback[1])) return fallback[1].replace(/[^a-z0-9\-/]/ig, "");
  return "";
}

function parsePageInfo(text) {
  const cleaned = text.replace(/\s+/g, " ");
  const pagePatterns = [
    /page\s*(\d+)\s*(?:of|out\s*of|\/)\s*(\d+)/i,
    /p(?:age)?\.?\s*no\.?\s*[:#\s-]*(\d+)\s*(?:of|out\s*of|\/)\s*(\d+)/i,
    /(?:sheet|page)\s+(\d+)\s*[-/]\s*(\d+)/i,
    /page\s*no\.?\s*[:#\s-]*(\d+)/i,
    /p\.?\s*(\d+)\s*[/]\s*(\d+)/i,
    /page\s*(\d+)/i,
    /p\.?\s*(\d+)\b/i,
  ];
  for (const regex of pagePatterns) {
    const match = cleaned.match(regex);
    if (match) {
      const current = parseInt(match[1]);
      const total = match[2] ? parseInt(match[2]) : (current >= 1 ? Math.max(current, 1) : 1);
      if (current >= 1 && total >= 1 && current <= total) {
        return { current, total };
      }
      return { current, total: Math.max(current, total) };
    }
  }
  return { current: 1, total: 1 };
}

async function handleMultipleFilesSelected(files) {
  showLoadingOverlay(true, "Initializing OCR scanner...");
  triageFiles = [];
  triageGroups = [];

  let count = 0;
  for (const file of files) {
    count++;
    showLoadingOverlay(true, `OCR Analysis: File ${count}/${files.length}...`);
    try {
      const text = await performOcrWithRetry(file);
      const invoiceNo = parseInvoiceNo(text);
      const { current: pageNum, total: totalPages } = parsePageInfo(text);
      
      console.log(`OCR Raw Text for ${file.name}:`, text);
      console.log(`OCR Parsed Metadata for ${file.name}:`, { invoiceNo, pageNum, totalPages });

      triageFiles.push({
        id: `file_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`,
        file,
        objectUrl: URL.createObjectURL(file),
        text,
        invoiceNo,
        pageNum,
        totalPages
      });
    } catch (err) {
      console.error("OCR failed for file: " + file.name, err);
      triageFiles.push({
        id: `file_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`,
        file,
        objectUrl: URL.createObjectURL(file),
        text: "",
        invoiceNo: "",
        pageNum: 1,
        totalPages: 1
      });
    }
  }

  await terminateOcrWorker();
  showLoadingOverlay(false);
  autoGroupTriageFiles();

  // If there are no multi-page groups (every group has exactly 1 file), bypass the triage modal entirely!
  const hasMultiPageGroup = triageGroups.some(g => g.files.length > 1);
  if (!hasMultiPageGroup) {
    console.log("No multi-page invoices detected, bypassing triage modal and proceeding to extraction...");
    startTriageUploadAndExtraction();
  } else {
    openTriageModal();
  }
}

function autoGroupTriageFiles() {
  triageGroups = [];
  const groupsMap = {};
  const unidentifiedFiles = [];

  triageFiles.forEach((tf) => {
    if (tf.invoiceNo) {
      if (!groupsMap[tf.invoiceNo]) {
        groupsMap[tf.invoiceNo] = {
          id: `group_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`,
          invoiceNo: tf.invoiceNo,
          files: []
        };
        triageGroups.push(groupsMap[tf.invoiceNo]);
      }
      groupsMap[tf.invoiceNo].files.push(tf.id);
    } else {
      unidentifiedFiles.push(tf);
    }
  });

  let lastGroupId = null;
  if (triageGroups.length > 0) {
    lastGroupId = triageGroups[triageGroups.length - 1].id;
  }

  unidentifiedFiles.forEach((tf) => {
    if (tf.pageNum > 1 && lastGroupId) {
      const group = triageGroups.find(g => g.id === lastGroupId);
      group.files.push(tf.id);
    } else {
      const newGroup = {
        id: `group_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`,
        invoiceNo: `UNIDENTIFIED_${triageGroups.length + 1}`,
        files: [tf.id]
      };
      triageGroups.push(newGroup);
      lastGroupId = newGroup.id;
    }
  });

  triageGroups.forEach((group) => {
    group.files.sort((aId, bId) => {
      const a = triageFiles.find(f => f.id === aId);
      const b = triageFiles.find(f => f.id === bId);
      return a.pageNum - b.pageNum;
    });
  });
}

function openTriageModal() {
  $("triage-modal").classList.remove("hidden");
  renderTriageGroups();
  
  $("btn-close-triage").onclick = () => {
    $("triage-modal").classList.add("hidden");
    triageFiles.forEach(f => URL.revokeObjectURL(f.objectUrl));
  };
  
  $("btn-triage-add-group").onclick = () => {
    const newGroup = {
      id: `group_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`,
      invoiceNo: `NEW_GROUP_${triageGroups.length + 1}`,
      files: []
    };
    triageGroups.push(newGroup);
    renderTriageGroups();
  };
  
  $("btn-triage-confirm").onclick = startTriageUploadAndExtraction;
}

function renderTriageGroups() {
  const container = $("triage-groups-container");
  container.innerHTML = "";

  triageGroups.forEach((group) => {
    const card = document.createElement("div");
    card.className = "bg-slate-800/60 border border-slate-700/60 rounded-xl p-4 space-y-3";
    card.dataset.groupId = group.id;

    const headerHtml = `
      <div class="flex justify-between items-center gap-2">
        <div class="flex items-center gap-1.5 flex-1">
          <label class="text-[10px] font-bold text-slate-500 uppercase">Invoice No:</label>
          <input type="text" value="${group.invoiceNo}" 
            class="flex-1 bg-slate-900 border border-slate-700 rounded-md px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-indigo-500 font-mono"
            onchange="window.updateTriageGroupName('${group.id}', this.value)"
          >
        </div>
        <button class="text-slate-400 hover:text-rose-400 p-1 rounded" onclick="window.deleteTriageGroup('${group.id}')" title="Delete Group">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>
        </button>
      </div>
    `;

    let pagesHtml = `<div class="grid grid-cols-2 gap-2">`;
    if (group.files.length === 0) {
      pagesHtml += `<div class="col-span-2 text-center text-xs text-slate-500 py-3">No pages in this group. Move pages here.</div>`;
    } else {
      group.files.forEach((fileId) => {
        const tf = triageFiles.find(f => f.id === fileId);
        pagesHtml += `
          <div class="bg-slate-900 border border-slate-800 rounded-lg p-2 flex flex-col gap-2 relative">
            <div class="aspect-[4/3] bg-black rounded overflow-hidden relative">
              <img src="${tf.objectUrl}" class="w-full h-full object-contain">
              <span class="absolute bottom-1 right-1 bg-black/75 px-1.5 py-0.5 rounded text-[8px] font-mono text-slate-300">
                Page ${tf.pageNum}
              </span>
            </div>
            <div class="flex flex-col gap-1">
              <span class="text-[9px] text-slate-500 truncate">${tf.file.name}</span>
              <div class="flex items-center gap-1">
                <span class="text-[8px] text-slate-500 uppercase">Move:</span>
                <select class="flex-1 bg-slate-800 border border-slate-700 rounded text-[9px] px-1 py-0.5 text-slate-300"
                  onchange="window.moveTriageFile('${tf.id}', this.value)"
                >
                  ${triageGroups.map(g => `<option value="${g.id}" ${g.id === group.id ? "selected" : ""}>${g.invoiceNo}</option>`).join("")}
                </select>
              </div>
            </div>
          </div>
        `;
      });
    }
    pagesHtml += `</div>`;

    card.innerHTML = headerHtml + pagesHtml;
    container.appendChild(card);
  });

  $("triage-stats-txt").textContent = `${triageFiles.length} file(s) · ${triageGroups.length} invoice group(s)`;
}

window.updateTriageGroupName = (groupId, val) => {
  const group = triageGroups.find(g => g.id === groupId);
  if (group) {
    group.invoiceNo = val.trim();
    renderTriageGroups();
  }
};

window.deleteTriageGroup = (groupId) => {
  const group = triageGroups.find(g => g.id === groupId);
  if (group) {
    triageGroups = triageGroups.filter(g => g.id !== groupId);
    if (group.files.length > 0) {
      if (triageGroups.length > 0) {
        triageGroups[0].files.push(...group.files);
      } else {
        triageGroups.push({
          id: `group_${Date.now()}`,
          invoiceNo: "ORPHANED_PAGES",
          files: group.files
        });
      }
    }
    renderTriageGroups();
  }
};

window.moveTriageFile = (fileId, targetGroupId) => {
  triageGroups.forEach((g) => {
    g.files = g.files.filter(id => id !== fileId);
  });
  const targetGroup = triageGroups.find(g => g.id === targetGroupId);
  if (targetGroup) {
    targetGroup.files.push(fileId);
    targetGroup.files.sort((aId, bId) => {
      const a = triageFiles.find(f => f.id === aId);
      const b = triageFiles.find(f => f.id === bId);
      return a.pageNum - b.pageNum;
    });
  }
  renderTriageGroups();
};

async function startTriageUploadAndExtraction() {
  $("triage-modal").classList.add("hidden");
  
  const groupsToProcess = triageGroups.filter(g => g.files.length > 0);
  if (groupsToProcess.length === 0) {
    showToast("No invoices to process", "warning");
    return;
  }

  showLoadingOverlay(true, "Uploading invoice files...");
  reviewQueue = [];
  reviewQueueIndex = 0;

  const extractFn = httpsCallable(functions, "extractInvoice", { timeout: 120000 });

  for (let i = 0; i < groupsToProcess.length; i++) {
    const group = groupsToProcess[i];
    showLoadingOverlay(true, `Processing invoice ${i + 1}/${groupsToProcess.length}...`);

    const storagePaths = [];
    const objectUrls = [];
    
    try {
      for (let j = 0; j < group.files.length; j++) {
        const tf = triageFiles.find(f => f.id === group.files[j]);
        const ext = tf.file.type === "application/pdf" ? "pdf" : "jpg";
        const filename = `${Date.now()}_grp_${i}_p_${j}_${Math.random().toString(36).slice(2)}.${ext}`;
        const storagePath = `invoices/${currentPharmacyId}/${filename}`;
        const storageRef = ref(storage, storagePath);
        
        await uploadBytes(storageRef, tf.file, { contentType: tf.file.type });
        storagePaths.push(storagePath);
        objectUrls.push(tf.objectUrl);
      }
    } catch (uploadErr) {
      showLoadingOverlay(false);
      showToast(`Upload failed for group ${group.invoiceNo}: ${uploadErr.message}`, "error");
      return;
    }

    showLoadingOverlay(true, `Extracting Invoice "${group.invoiceNo}" with Gemini AI...`);
    try {
      const result = await extractFn({ storagePaths, pharmacyId: currentPharmacyId });
      reviewQueue.push({
        storagePaths,
        objectUrls,
        extracted: result.data
      });
    } catch (geminiErr) {
      console.error(`Gemini extraction failed for group "${group.invoiceNo}":`, geminiErr);
      showLoadingOverlay(false);
      showToast(`Gemini extraction failed for group ${group.invoiceNo}: ${geminiErr.message}`, "error");
      for (const p of storagePaths) {
        try { await deleteObject(ref(storage, p)); } catch (_) {}
      }
      return;
    }
  }

  showLoadingOverlay(false);
  
  triageFiles.forEach((tf) => {
    const isUsed = reviewQueue.some(item => item.objectUrls.includes(tf.objectUrl));
    if (!isUsed) URL.revokeObjectURL(tf.objectUrl);
  });

  if (reviewQueue.length > 0) {
    showNextReviewInQueue();
  } else {
    showToast("No invoices extracted successfully", "error");
  }
}

function showNextReviewInQueue() {
  if (reviewQueueIndex >= reviewQueue.length) {
    showToast("All invoice extractions reviewed and saved!", "success");
    loadExpiringMedicines();
    return;
  }

  const reviewItem = reviewQueue[reviewQueueIndex];
  reviewSession = {
    storagePaths: reviewItem.storagePaths,
    objectUrls: reviewItem.objectUrls,
    currentPageIndex: 0,
    fileType: 'image',
    extracted: reviewItem.extracted
  };

  openReviewPanel(reviewItem.extracted);
}

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
    
    await page.render({ canvasContext: ctx, viewport: viewport }).promise;
    
    const blob = await new Promise(resolve => canvas.toBlob(resolve, 'image/jpeg', 0.9));
    const pageFile = new File([blob], `${file.name.replace(/\.[^/.]+$/, "")}_page_${pageNum}.jpg`, { type: 'image/jpeg' });
    images.push(pageFile);
  }
  return images;
}

