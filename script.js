/* =================== RxExpiry – script.js =================== */
/* Multi-tenant pharmacy expiry tracker
   Flow: Auth → Capture → Quality Check → extractInvoice CF (Gemini) → Review → Save
   Firebase v10+ modular SDK via CDN (ES module)                     */

// ─── Firebase Config (replace with your project values) ────────────
const FIREBASE_CONFIG = {
    apiKey: "AIzaSyDFSBF3cgMADrs_hp80Z7OOUyPaUPlxxiE",
    authDomain: "medsof-17a68.firebaseapp.com",
    projectId: "medsof-17a68",
    storageBucket: "medsof-17a68.firebasestorage.app",
    messagingSenderId: "727412394149",
    appId: "1:727412394149:web:2fc2fcd7689c5e2392a54c"
};

// ─── State ─────────────────────────────────────────────────────────
const State = {
    user: null,
    pharmacyId: 'city-pharma',
    role: 'owner',
    currentView: 'view-home',
    cameraStream: null,
    currentImageFile: null,
    currentImageBlob: null,
    currentImageHash: null,
    extractedData: null,
    medicines: [],
    distributors: [],
    staff: [],
    invoices: [],
    selectedBatch: null,
    isDark: true,
    // Batch queue for multi-image extraction
    imageQueue: [],          // Array of { file, blob, type } objects
    extractedQueue: [],      // Array of extraction results (filled as each image is processed)
    currentQueueIndex: 0,    // Which image in the queue is currently being reviewed
    batchId: null,           // Shared batch ID for all pages in a PDF upload
    // Bulk selection
    selectedMedIds: new Set(),
    selectMode: false,
    // Firebase refs
    _app: null,
    _auth: null,
    _db: null,
    _storage: null,
    _functions: null,
    _confirmationResult: null,
    _recaptcha: null
};

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

// ─── Image Hash (pHash) Utilities ──────────────────────────────────
async function computeImageHash(fileOrBlob) {
    return new Promise((resolve) => {
        const img = new Image();
        img.onload = () => {
            const SIZE = 8;
            const cvs = document.createElement('canvas');
            cvs.width = SIZE; cvs.height = SIZE;
            const ctx = cvs.getContext('2d');
            ctx.drawImage(img, 0, 0, SIZE, SIZE);
            const px = ctx.getImageData(0, 0, SIZE, SIZE).data;
            // Grayscale
            const gray = [];
            for (let i = 0; i < SIZE * SIZE; i++) {
                const j = i * 4;
                gray.push(0.299 * px[j] + 0.587 * px[j+1] + 0.114 * px[j+2]);
            }
            // Average
            const avg = gray.reduce((s, v) => s + v, 0) / gray.length;
            // Build 64-bit hash string
            let hash = '';
            for (let i = 0; i < gray.length; i++) hash += gray[i] >= avg ? '1' : '0';
            URL.revokeObjectURL(img.src);
            resolve(hash);
        };
        img.onerror = () => resolve(null);
        const src = fileOrBlob instanceof Blob ? URL.createObjectURL(fileOrBlob) : fileOrBlob;
        img.src = src;
    });
}

function hammingDistance(a, b) {
    if (!a || !b || a.length !== b.length) return Infinity;
    let d = 0;
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) d++;
    return d;
}

async function checkImageDuplicate(hash) {
    if (!hash || !isFirebaseReady()) return null;
    try {
        const { collection, query, orderBy, limit, getDocs } = State._fbFirestore;
        const q = query(
            collection(State._db, `pharmacies/${State.pharmacyId}/invoices`),
            orderBy('capturedAt', 'desc'),
            limit(50)
        );
        const snap = await getDocs(q);
        for (const doc of snap.docs) {
            const inv = doc.data();
            if (inv.imageHash && hammingDistance(hash, inv.imageHash) <= 8) {
                return { invoiceId: doc.id, distributor: inv.distributor, invoiceNumber: inv.invoiceNumber, capturedAt: inv.capturedAt };
            }
        }
    } catch (e) {
        console.warn('[RxExpiry] Image hash check failed:', e);
    }
    return null;
}

async function checkInvoiceDuplicate(distributor, invoiceNumber) {
    if (!distributor || !invoiceNumber || !isFirebaseReady()) return null;
    try {
        const { collection, query, where, getDocs, limit } = State._fbFirestore;
        const q = query(
            collection(State._db, `pharmacies/${State.pharmacyId}/invoices`),
            where('distributor', '==', distributor),
            where('invoiceNumber', '==', invoiceNumber),
            limit(1)
        );
        const snap = await getDocs(q);
        if (!snap.empty) {
            const doc = snap.docs[0];
            return { invoiceId: doc.id, distributor: doc.data().distributor, invoiceNumber: doc.data().invoiceNumber, capturedAt: doc.data().capturedAt };
        }
    } catch (e) {
        console.warn('[RxExpiry] Invoice duplicate check failed:', e);
    }
    return null;
}

function isFirebaseReady() {
    return State._auth && State._db && State._storage && State._functions;
}

function isUserAuthenticated() {
    return isFirebaseReady() && !!State._auth.currentUser;
}

// ─── Bootstrap ─────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
    // Try to init Firebase
    try {
        await initFirebase();
        console.log('[RxExpiry] Firebase connected');
    } catch (e) {
        console.warn('[RxExpiry] Firebase not configured — running in demo mode', e);
        loadDemoData();
    }

    bindNavigation();
    bindAuth();
    bindCapture();
    bindReview();
    bindSearch();
    bindSettings();
    bindDistributorForm();
    bindThemeToggle();
    bindExport();

    showView('auth-screen');
});

async function initFirebase() {
    const { initializeApp } = await import("https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js");
    const auth = await import("https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js");
    const firestore = await import("https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js");
    const storage = await import("https://www.gstatic.com/firebasejs/10.12.0/firebase-storage.js");
    const functions = await import("https://www.gstatic.com/firebasejs/10.12.0/firebase-functions.js");

    State._app = initializeApp(FIREBASE_CONFIG);
    State._auth = auth.getAuth(State._app);
    State._db = firestore.getFirestore(State._app);
    State._storage = storage.getStorage(State._app);
    State._functions = functions.getFunctions(State._app);

    // Store auth instance + classes needed at runtime
    State._fbAuth = auth;
    State._fbFirestore = firestore;
    State._fbStorage = storage;
    State._fbFunctions = functions;
    State._RecaptchaVerifier = auth.RecaptchaVerifier;
    State._signInWithPhoneNumber = auth.signInWithPhoneNumber;

    // Listen for auth state changes
    auth.onAuthStateChanged(State._auth, (user) => {
        if (user) {
            console.log('[RxExpiry] Auth state:', user.phoneNumber);
        }
    });
}

async function fetchFirestoreData() {
    if (!isUserAuthenticated()) return;
    try {
        const { collection, getDocs } = State._fbFirestore;
        const pharmacyId = State.pharmacyId;
        const medsSnap = await getDocs(collection(State._db, `pharmacies/${pharmacyId}/medicines`));
        State.medicines = [];
        medsSnap.forEach(doc => {
            State.medicines.push({ id: doc.id, ...doc.data() });
        });
        const invoicesSnap = await getDocs(collection(State._db, `pharmacies/${pharmacyId}/invoices`));
        State.invoices = [];
        invoicesSnap.forEach(doc => {
            State.invoices.push({ id: doc.id, ...doc.data() });
        });
        // Fetch distributors
        const distSnap = await getDocs(collection(State._db, `pharmacies/${pharmacyId}/distributors`));
        State.distributors = [];
        distSnap.forEach(doc => {
            State.distributors.push({ id: doc.id, ...doc.data() });
        });
        renderExpiringList();
        updateStats();
        console.log(`[RxExpiry] Loaded ${State.medicines.length} medicines, ${State.invoices.length} invoices, ${State.distributors.length} distributors from Firestore`);
    } catch (e) {
        console.error('[RxExpiry] Firestore fetch failed:', e);
    }
}

// ─── Diagnostic: test Firestore write from console ─────────────
window.testFirestoreWrite = async function() {
    console.log('[TEST] Auth state:', State._auth.currentUser?.uid, 'isAnonymous:', State._auth.currentUser?.isAnonymous);
    console.log('[TEST] Firebase ready:', !!isFirebaseReady());

    // Test A: Direct client SDK write
    try {
        const { doc, setDoc, getDoc } = State._fbFirestore;
        const testRef = doc(State._db, 'pharmacies/city-pharma/diagnostics/test-write');
        await setDoc(testRef, { test: true, source: 'client-sdk', timestamp: new Date().toISOString() });
        const snap = await getDoc(testRef);
        console.log('[TEST-A] Client SDK write SUCCEEDED:', snap.data());
    } catch (e) {
        console.error('[TEST-A] Client SDK write FAILED:', e.code, e.message);
    }

    // Test B: Cloud Function (Admin SDK) write
    try {
        const { httpsCallable } = State._fbFunctions;
        const fn = httpsCallable(State._functions, 'testFirestoreWrite');
        const result = await fn({});
        console.log('[TEST-B] Cloud Function result:', JSON.stringify(result.data, null, 2));
    } catch (e) {
        console.error('[TEST-B] Cloud Function FAILED:', e.code, e.message);
    }
};

function loadDemoData() {
    State.medicines = getDemoMedicines();
    State.distributors = getDemoDistributors();
    State.invoices = getDemoInvoices();
    State.staff = getDemoStaff();
}

// ═══════════════════════════════════════════════════════════════════
// 1. NAVIGATION
// ═══════════════════════════════════════════════════════════════════
function bindNavigation() {
    $$('.nav-tab').forEach(btn => {
        btn.addEventListener('click', () => {
            $$('.nav-tab').forEach(b => {
                b.classList.remove('text-indigo-500');
                b.classList.add('text-slate-500');
            });
            btn.classList.add('text-indigo-500');
            btn.classList.remove('text-slate-500');
            showView(btn.dataset.target);
        });
    });
}

function showView(viewId) {
    if (viewId === 'auth-screen') {
        $('#auth-screen').classList.remove('hidden');
        $('#auth-screen').classList.add('flex');
        $('#app-workspace').classList.add('hidden');
    } else {
        $('#auth-screen').classList.add('hidden');
        $('#auth-screen').classList.remove('flex');
        $('#app-workspace').classList.remove('hidden');
        $$('.view-pane').forEach(v => v.classList.add('hidden'));
        const pane = $(`#${viewId}`);
        if (pane) pane.classList.remove('hidden');
        State.currentView = viewId;
        if (viewId === 'view-home') renderExpiringList();
        if (viewId === 'view-settings') { updateStats(); loadStaffList(); }
        if (viewId === 'view-distributors') renderDistributors();
    }
}

// ═══════════════════════════════════════════════════════════════════
// 2. TOAST
// ═══════════════════════════════════════════════════════════════════
function showToast(msg, type = 'indigo') {
    const colors = { indigo: 'bg-indigo-500', green: 'bg-emerald-500', red: 'bg-rose-500', amber: 'bg-amber-500' };
    const toast = $('#toast');
    $('#toast-icon').className = `w-2 h-2 rounded-full ${colors[type] || colors.indigo}`;
    $('#toast-text').textContent = msg;
    toast.classList.remove('translate-y-[-100px]', 'opacity-0');
    toast.classList.add('toast-enter');
    clearTimeout(toast._t);
    toast._t = setTimeout(() => {
        toast.classList.remove('toast-enter');
        toast.classList.add('translate-y-[-100px]', 'opacity-0');
    }, 3000);
}

// ═══════════════════════════════════════════════════════════════════
// 3. AUTHENTICATION (Firebase Phone OTP)
// ═══════════════════════════════════════════════════════════════════
function bindAuth() {
    $('#auth-pharmacy-id').addEventListener('change', (e) => {
        $('#new-pharmacy-form').classList.toggle('hidden', e.target.value !== 'new-pharmacy');
        State.pharmacyId = e.target.value;
    });

    $$('.role-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            $$('.role-btn').forEach(b => {
                b.classList.remove('bg-indigo-600', 'text-white', 'shadow-sm');
                b.classList.add('text-slate-400');
            });
            btn.classList.add('bg-indigo-600', 'text-white', 'shadow-sm');
            btn.classList.remove('text-slate-400');
            State.role = btn.dataset.role;
        });
    });

    $('#auth-submit-btn').addEventListener('click', handleAuthSubmit);
    $('#auth-logout-btn').addEventListener('click', handleLogout);
}

async function handleAuthSubmit() {
    const btn = $('#auth-submit-btn');
    const phone = $('#auth-phone').value.replace(/\s/g, '');
    const otpVisible = !$('#otp-container').classList.contains('hidden');

    if (!phone || phone.length < 10) {
        showToast('Enter a valid 10-digit phone number', 'red');
        return;
    }

    if (!otpVisible) {
        // ── Step 1: Send OTP ──
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner spinner-white inline-block"></span> Sending...';

        if (isFirebaseReady()) {
            try {
                if (!State._recaptcha) {
                    State._recaptcha = new State._RecaptchaVerifier(State._auth, 'recaptcha-container', { size: 'invisible' });
                }
                State._confirmationResult = await State._signInWithPhoneNumber(State._auth, `+91${phone}`, State._recaptcha);
                showToast('OTP sent', 'green');
            } catch (e) {
                console.error('[RxExpiry] OTP send error:', e);
                showToast('OTP send failed — check Firebase config', 'red');
                btn.disabled = false;
                btn.textContent = 'Send OTP';
                return;
            }
        } else {
            showToast('Demo mode: enter any 4+ digit OTP', 'amber');
        }

        $('#otp-container').classList.remove('hidden');
        btn.textContent = 'Verify OTP';
        btn.disabled = false;

    } else {
        // ── Step 2: Verify OTP ──
        const otp = $('#auth-otp').value.trim();
        if (!otp || otp.length < 4) { showToast('Enter the OTP code', 'red'); return; }

        btn.disabled = true;
        btn.innerHTML = '<span class="spinner spinner-white inline-block"></span> Verifying...';

        if (isFirebaseReady() && State._confirmationResult) {
            try {
                await State._confirmationResult.confirm(otp);
                loginSuccess();
            } catch (e) {
                console.error('[RxExpiry] OTP verify error:', e);
                showToast('Invalid OTP', 'red');
                btn.disabled = false;
                btn.textContent = 'Verify OTP';
            }
        } else {
            // Demo fallback
            if (otp.length >= 4) {
                loginSuccess();
            } else {
                showToast('Enter at least 4 digits', 'red');
                btn.disabled = false;
                btn.textContent = 'Verify OTP';
            }
        }
    }
}

async function loginSuccess() {
    if (isFirebaseReady() && !State._auth.currentUser) {
        try {
            const cred = await State._fbAuth.signInAnonymously(State._auth);
            console.log('[RxExpiry] Anonymous auth OK, uid:', cred.user?.uid);
        } catch (e) {
            console.warn('[RxExpiry] Anonymous auth failed:', e.code, e.message);
        }
    }
    State.user = {
        phone: $('#auth-phone').value,
        role: State.role,
        pharmacyId: State.pharmacyId,
        uid: isFirebaseReady() && State._auth.currentUser ? State._auth.currentUser.uid : 'demo-user'
    };
    console.log('[RxExpiry] Login success — authenticated:', isUserAuthenticated(), 'uid:', State.user.uid);

    // Ensure staff record exists so isStaff() in Firestore rules passes
    if (isUserAuthenticated()) {
        try {
            const { doc, setDoc, getDoc, serverTimestamp } = State._fbFirestore;
            const staffRef = doc(State._db, `pharmacies/${State.pharmacyId}/staff/${State.user.uid}`);
            const staffSnap = await getDoc(staffRef);
            if (!staffSnap.exists()) {
                await setDoc(staffRef, {
                    uid: State.user.uid,
                    phone: State.user.phone,
                    role: State.user.role,
                    pharmacyId: State.pharmacyId,
                    createdAt: serverTimestamp()
                });
                console.log('[RxExpiry] Created staff record for uid:', State.user.uid, 'at pharmacies/' + State.pharmacyId);
            } else {
                console.log('[RxExpiry] Staff record exists for uid:', State.user.uid);
            }
        } catch (e) {
            console.warn('[RxExpiry] Staff record check/create failed:', e.code, e.message);
        }
    }

    $('#header-pharmacy-name').textContent = getPharmacyLabel(State.pharmacyId);
    $('#header-user-status').textContent = State.role === 'owner' ? 'Owner Mode' : 'Staff Mode';
    showView('view-home');
    showToast(`Welcome! (${isUserAuthenticated() ? 'Live' : 'Demo'} mode)`, 'green');
    await fetchFirestoreData();
}

async function handleLogout() {
    if (isFirebaseReady()) {
        try { await State._fbAuth.signOut(State._auth); } catch (e) {}
    }
    stopCamera();
    State.user = null;
    State._confirmationResult = null;
    showView('auth-screen');
    showToast('Logged out', 'amber');
}

function getPharmacyLabel(id) {
    return { 'city-pharma': 'City Pharmacy', 'metro-meds': 'Metro Medicines', 'care-first': 'Care First Wellness' }[id] || id;
}

// ═══════════════════════════════════════════════════════════════════
// 4. CAPTURE (Camera / Gallery / PDF)
// ═══════════════════════════════════════════════════════════════════
function bindCapture() {
    $('#btn-camera-scan').addEventListener('click', startCamera);
    $('#btn-camera-capture').addEventListener('click', captureFromCamera);
    $('#btn-close-camera').addEventListener('click', stopCamera);
    $('#upload-gallery').addEventListener('change', handleGalleryUpload);
    $('#upload-pdf').addEventListener('change', handlePdfUpload);
    $('#upload-zip').addEventListener('change', handleZipUpload);
}

async function startCamera() {
    try {
        const stream = await navigator.mediaDevices.getUserMedia({
            video: { facingMode: 'environment', width: { ideal: 1920 }, height: { ideal: 1080 } }
        });
        State.cameraStream = stream;
        $('#camera-stream').srcObject = stream;
        $('#camera-feed-container').classList.remove('hidden');
        $('#btn-camera-scan').classList.add('hidden');
    } catch (e) {
        showToast('Camera denied — use Gallery upload', 'red');
    }
}

function stopCamera() {
    if (State.cameraStream) { State.cameraStream.getTracks().forEach(t => t.stop()); State.cameraStream = null; }
    $('#camera-feed-container').classList.add('hidden');
    $('#btn-camera-scan').classList.remove('hidden');
}

async function captureFromCamera() {
    const video = $('#camera-stream');
    const canvas = document.createElement('canvas');
    canvas.width = video.videoWidth; canvas.height = video.videoHeight;
    canvas.getContext('2d').drawImage(video, 0, 0);
    stopCamera();
    canvas.toBlob(async (blob) => {
        if (!blob) { showToast('Capture failed', 'red'); return; }
        const file = new File([blob], `capture_${Date.now()}.jpg`, { type: 'image/jpeg' });
        State.currentImageFile = file;
        State.currentImageBlob = blob;
        await runQualityCheck(blob, 'image');
    }, 'image/jpeg', 0.92);
}

async function handleGalleryUpload(e) {
    const files = Array.from(e.target.files);
    if (!files.length) return;

    if (files.length === 1) {
        // Single file — original flow
        const file = files[0];
        State.currentImageFile = file;
        State.currentImageBlob = new Blob([await file.arrayBuffer()], { type: file.type });
        await runQualityCheck(State.currentImageBlob, 'image');
    } else {
        // Multiple files — ONE invoice group (staff selected photos of same invoice)
        const batchId = `gallery_${Date.now()}_${Math.random().toString(36).slice(2,6)}`;
        State.batchId = batchId;
        await processGalleryGroup(files, batchId);
    }
    e.target.value = '';
}

// ═══════════════════════════════════════════════════════════════════
// 5c. GALLERY GROUP — Multiple photos treated as ONE invoice
// Extracts each image, then merges into a single invoice for review
// ═══════════════════════════════════════════════════════════════════
async function processGalleryGroup(files, batchId) {
    const total = files.length;
    showToast(`${total} photos selected — extracting as one invoice...`, 'indigo');
    setBatchPhase('extract');
    updateBatchProgress(0, total, `Extracting image 1/${total}...`);

    const results = [];

    for (let i = 0; i < total; i++) {
        const file = files[i];
        const blob = new Blob([await file.arrayBuffer()], { type: file.type });
        updateBatchProgress(i, total, `Extracting image ${i + 1}/${total}...`, `${i}/${total} complete`);

        try {
            const result = await extractSingleImage(file, 'image', batchId, i, {
                totalPages: total,
                originalName: file.name
            });
            if (result && result.lineItems && result.lineItems.length > 0) {
                results.push({ ...result, _pageIndex: i, _blob: blob, _file: file });
                console.log(`[RxExpiry] Gallery group[${i}] extracted ${result.lineItems.length} items`);
            } else {
                console.warn(`[RxExpiry] Gallery group[${i}] returned no items — skipping`);
            }
        } catch (e) {
            console.error(`[RxExpiry] Gallery group[${i}] extraction failed:`, e);
        }
    }

    updateBatchProgress(total, total, 'Merging pages into one invoice...');

    if (results.length === 0) {
        hideBatchProgress();
        showToast('No items could be extracted from selected photos', 'red');
        return;
    }

    // Merge all extracted pages into a single invoice
    const merged = mergeGalleryResults(results);
    merged._batchId = batchId;
    merged._pageCount = results.length;

    hideBatchProgress();
    State.extractedQueue = [merged];
    State.currentQueueIndex = 0;
    showNextReview();
}

// ═══════════════════════════════════════════════════════════════════
// 5d. MERGE GALLERY RESULTS — Combine multiple extractions into one
// Keeps first page's metadata; uses whichever page has totals block
// ═══════════════════════════════════════════════════════════════════
function mergeGalleryResults(results) {
    if (results.length === 1) return results[0];

    // Start with first result as base
    const base = { ...results[0] };

    // Combine all lineItems (tag each with source page for traceability)
    const allLineItems = [];
    for (const r of results) {
        const items = (r.lineItems || []).map(item => ({
            ...item,
            _fromPage: r._pageIndex
        }));
        allLineItems.push(...items);
    }
    base.lineItems = allLineItems;

    // Pick best metadata: use whichever page has non-zero/non-empty values
    // Totals (invoiceTotal, etc.) are typically on the last page with the summary block
    for (let i = 1; i < results.length; i++) {
        const r = results[i];
        // Prefer later page's totals if they have values
        if (r.invoiceTotal && (!base.invoiceTotal || base.invoiceTotal === 0)) base.invoiceTotal = r.invoiceTotal;
        if (r.totalTaxableAmount && (!base.totalTaxableAmount || base.totalTaxableAmount === 0)) base.totalTaxableAmount = r.totalTaxableAmount;
        if (r.cgstTotal && (!base.cgstTotal || base.cgstTotal === 0)) base.cgstTotal = r.cgstTotal;
        if (r.sgstTotal && (!base.sgstTotal || base.sgstTotal === 0)) base.sgstTotal = r.sgstTotal;
        if (r.schemeDiscount && (!base.schemeDiscount || base.schemeDiscount === 0)) base.schemeDiscount = r.schemeDiscount;
        if (r.cashDiscount && (!base.cashDiscount || base.cashDiscount === 0)) base.cashDiscount = r.cashDiscount;
        if (r.roundOff && (!base.roundOff || base.roundOff === 0)) base.roundOff = r.roundOff;

        // Keep first page's distributor/invoiceNumber/date (header info)
        // But if first page is missing them, grab from later pages
        if (!base.distributor && r.distributor) base.distributor = r.distributor;
        if (!base.invoiceNumber && r.invoiceNumber) base.invoiceNumber = r.invoiceNumber;
        if (!base.invoiceDate && r.invoiceDate) base.invoiceDate = r.invoiceDate;
        if (!base.buyerName && r.buyerName) base.buyerName = r.buyerName;
    }

    // Merge pageInfo if available
    const pageInfoEntries = results.filter(r => r.captureQuality?.pageInfo).map(r => r.captureQuality.pageInfo);
    if (pageInfoEntries.length > 0) {
        const maxPage = Math.max(...pageInfoEntries.map(p => p.current || 0));
        const totalPages = Math.max(...pageInfoEntries.map(p => p.total || 0));
        base.captureQuality = base.captureQuality || {};
        base.captureQuality.pageInfo = { current: maxPage, total: totalPages };
    }

    // Store page blobs for image preview (use first page)
    base._blob = results[0]._blob;
    base._file = results[0]._file;

    console.log(`[RxExpiry] mergeGalleryResults: ${results.length} pages → ${allLineItems.length} total line items`);
    return base;
}

async function handlePdfUpload(e) {
    const files = Array.from(e.target.files);
    if (!files.length) return;

    // Generate a shared batch ID for all pages from this upload
    const batchId = `batch_${Date.now()}_${Math.random().toString(36).slice(2,6)}`;
    State.batchId = batchId;

    const allPages = [];

    for (let f = 0; f < files.length; f++) {
        const file = files[f];
        const isSmall = file.size <= 5 * 1024 * 1024 && files.length === 1;

        if (isSmall) {
            // Small single PDF — send whole, no splitting
            State.currentImageFile = file;
            await runQualityCheck(file, 'pdf');
            e.target.value = '';
            return;
        }

        // Split this PDF into page images
        showToast(`Splitting PDF ${f + 1}/${files.length}...`, 'indigo');
        setBatchPhase('split');
        try {
            const pages = await splitPdfToImages(file);
            allPages.push(...pages);
        } catch (err) {
            console.error(`[RxExpiry] PDF ${f + 1} split failed:`, err);
            showToast(`PDF split failed: ${err.message}`, 'red');
        }
    }

    if (allPages.length === 0) {
        showToast('No pages produced from PDFs', 'red');
        e.target.value = '';
        return;
    }

    // Phase 2: Quality check each page
    setBatchPhase('quality');
    const qualifiedPages = [];
    let blurRejects = 0;

    for (let i = 0; i < allPages.length; i++) {
        const page = allPages[i];
        updateBatchProgress(i + 1, allPages.length, `Checking quality of page ${i + 1}/${allPages.length}...`);

        const q = await checkPageQuality(page.blob);
        if (q.pass) {
            qualifiedPages.push({
                ...page,
                batchId,
                pageIndex: i,
                _quality: { blur: q.blurScore, exp: q.expScore }
            });
        } else {
            blurRejects++;
            console.warn(`[RxExpiry] Page ${i + 1} rejected: ${q.reason}`);
        }
        // Yield to browser every 10 pages
        if ((i + 1) % 10 === 0) await new Promise(r => setTimeout(r, 0));
    }

    if (blurRejects > 0) {
        showToast(`${blurRejects} pages failed quality check — skipping`, 'amber');
    }

    if (qualifiedPages.length === 0) {
        showToast('All pages failed quality check', 'red');
        e.target.value = '';
        return;
    }

    // Phase 3: Queue for extraction
    showToast(`${qualifiedPages.length} pages ready — extracting...`, 'green');
    State.imageQueue = qualifiedPages;
    State.extractedQueue = [];
    State.currentQueueIndex = 0;
    processNextInQueue();

    e.target.value = '';
}

async function handleZipUpload(e) {
    const file = e.target.files[0];
    if (!file) return;

    const panel = $('#precheck-feedback-panel');
    panel.classList.remove('hidden');
    $('#precheck-status-msg').textContent = 'Extracting images from ZIP...';
    $('#precheck-status-msg').className = 'text-xs font-semibold py-1.5 px-3 rounded text-center text-indigo-400';
    $('#precheck-blur-val').textContent = 'Extracting...';
    $('#precheck-blur-bar').style.width = '0%';

    try {
        const pages = await extractZipToImages(file);
        if (pages.length === 0) {
            showToast('No images found in ZIP', 'red');
            panel.classList.add('hidden');
            e.target.value = '';
            return;
        }
        showToast(`Extracted ${pages.length} images from ZIP — processing...`, 'green');
        State.imageQueue = pages;
        State.extractedQueue = [];
        State.currentQueueIndex = 0;
        panel.classList.add('hidden');
        processNextInQueue();
    } catch (err) {
        console.error('[RxExpiry] ZIP extraction failed:', err);
        showToast('ZIP extraction failed: ' + err.message, 'red');
        panel.classList.add('hidden');
    }
    e.target.value = '';
}

// ═══════════════════════════════════════════════════════════════════
// 5. CLIENT-SIDE QUALITY CHECK (Blur + Exposure — pure code, no AI)
// ═══════════════════════════════════════════════════════════════════
async function runQualityCheck(file, type) {
    const panel = $('#precheck-feedback-panel');
    panel.classList.remove('hidden');
    $('#precheck-blur-val').textContent = 'Calculating...';
    $('#precheck-exposure-val').textContent = 'Calculating...';
    $('#precheck-blur-bar').style.width = '0%';
    $('#precheck-exposure-bar').style.width = '0%';
    const statusEl = $('#precheck-status-msg');
    statusEl.textContent = '';
    statusEl.className = 'text-xs font-semibold py-1.5 px-3 rounded text-center';

    if (type === 'pdf') {
        // No size cap — large PDFs are split client-side before reaching here
        updateBar('blur', 1, 'N/A (PDF)');
        updateBar('exposure', 1, 'N/A (PDF)');
        passPrecheck('PDF passed validation.');
        return;
    }

    try {
        const img = await loadImage(file);
        const W = 640, H = Math.round((img.height / img.width) * W);
        const cvs = document.createElement('canvas');
        cvs.width = W; cvs.height = H;
        const ctx = cvs.getContext('2d');
        ctx.drawImage(img, 0, 0, W, H);
        const px = ctx.getImageData(0, 0, W, H).data;

        // Luminance
        const gray = new Float32Array(W * H);
        for (let i = 0; i < W * H; i++) {
            const j = i * 4;
            gray[i] = 0.299 * px[j] + 0.587 * px[j+1] + 0.114 * px[j+2];
        }

        // Laplacian variance (blur detection)
        let lapSum = 0, lapN = 0;
        for (let y = 1; y < H-1; y++) {
            for (let x = 1; x < W-1; x++) {
                const c = gray[y*W+x];
                const l = -4*c + gray[(y-1)*W+x] + gray[(y+1)*W+x] + gray[y*W+x-1] + gray[y*W+x+1];
                lapSum += l*l; lapN++;
            }
        }
        const blurVar = lapSum / lapN;
        const blurScore = Math.min(blurVar / 100, 1);
        updateBar('blur', blurScore, blurVar.toFixed(1));

        // Exposure (avg luminance)
        let totalLum = 0;
        for (let i = 0; i < W*H; i++) totalLum += gray[i];
        const avgLum = totalLum / (W * H);
        let expScore = avgLum >= 60 && avgLum <= 200 ? 1 :
                       avgLum < 60 ? avgLum/60 : (255-avgLum)/55;
        expScore = Math.max(0, Math.min(1, expScore));
        updateBar('exposure', expScore, `Avg: ${avgLum.toFixed(0)}`);

        if (blurScore < 0.3 || expScore < 0.3) {
            const issues = [];
            if (blurScore < 0.3) issues.push('too blurry');
            if (expScore < 0.3) issues.push(avgLum < 60 ? 'too dark' : 'too bright');
            failPrecheck(`Please retake: ${issues.join(' and ')}.`);
        } else {
            passPrecheck('Image quality OK. Uploading to extractor...');
        }
    } catch (e) {
        console.error('Quality check error:', e);
        failPrecheck('Could not analyze image.');
    }
}

function updateBar(type, score, label) {
    const pct = Math.round(score * 100);
    $(`#precheck-${type}-val`).textContent = `${label} (${pct}%)`;
    const bar = $(`#precheck-${type}-bar`);
    bar.style.width = `${pct}%`;
    bar.className = `h-full transition-all duration-300 ${score < 0.3 ? 'bg-rose-500' : score < 0.7 ? 'bg-amber-500' : 'bg-emerald-500 precheck-bar-glow'}`;
}

function failPrecheck(msg) {
    const el = $('#precheck-status-msg');
    el.innerHTML = `${msg} <button onclick="window.retryCapture()" class="ml-2 underline font-bold">Retry</button>`;
    el.className = 'text-xs font-semibold py-1.5 px-3 rounded text-center bg-rose-500/15 text-rose-400 border border-rose-500/30';
}

function passPrecheck(msg) {
    const el = $('#precheck-status-msg');
    el.textContent = msg;
    el.className = 'text-xs font-semibold py-1.5 px-3 rounded text-center bg-emerald-500/15 text-emerald-400 border border-emerald-500/30';
    setTimeout(() => {
        $('#precheck-feedback-panel').classList.add('hidden');
        sendToExtractInvoice();
    }, 1200);
}

window.retryCapture = () => {
    $('#precheck-feedback-panel').classList.add('hidden');
    State.currentImageFile = null; State.currentImageBlob = null;
};

// ═══════════════════════════════════════════════════════════════════
// 5a0. BATCH-MODE QUALITY CHECK — Runs on JPEG blobs from PDF split
// Returns { pass: bool, blurScore, expScore, reason }
// ═══════════════════════════════════════════════════════════════════
async function checkPageQuality(blob) {
    try {
        const img = await loadImage(blob);
        const W = 640, H = Math.round((img.height / img.width) * W);
        const cvs = document.createElement('canvas');
        cvs.width = W; cvs.height = H;
        const ctx = cvs.getContext('2d');
        ctx.drawImage(img, 0, 0, W, H);
        const px = ctx.getImageData(0, 0, W, H).data;

        const gray = new Float32Array(W * H);
        for (let i = 0; i < W * H; i++) {
            const j = i * 4;
            gray[i] = 0.299 * px[j] + 0.587 * px[j+1] + 0.114 * px[j+2];
        }

        // Laplacian variance (blur)
        let lapSum = 0, lapN = 0;
        for (let y = 1; y < H-1; y++) {
            for (let x = 1; x < W-1; x++) {
                const c = gray[y*W+x];
                const l = -4*c + gray[(y-1)*W+x] + gray[(y+1)*W+x] + gray[y*W+x-1] + gray[y*W+x+1];
                lapSum += l*l; lapN++;
            }
        }
        const blurScore = Math.min((lapSum / lapN) / 100, 1);

        // Exposure (avg luminance)
        let totalLum = 0;
        for (let i = 0; i < W*H; i++) totalLum += gray[i];
        const avgLum = totalLum / (W * H);
        let expScore = avgLum >= 60 && avgLum <= 200 ? 1 :
                       avgLum < 60 ? avgLum/60 : (255-avgLum)/55;
        expScore = Math.max(0, Math.min(1, expScore));

        if (blurScore < 0.3 || expScore < 0.3) {
            const reasons = [];
            if (blurScore < 0.3) reasons.push('blurry');
            if (expScore < 0.3) reasons.push(avgLum < 60 ? 'dark' : 'bright');
            return { pass: false, blurScore, expScore, reason: reasons.join(', ') };
        }
        return { pass: true, blurScore, expScore, reason: null };
    } catch {
        return { pass: true, blurScore: 1, expScore: 1, reason: null };
    }
}

// ═══════════════════════════════════════════════════════════════════
// 5a. CLIENT-SIDE PDF SPLITTER — Render each page to high-res JPEG
// ═══════════════════════════════════════════════════════════════════
async function splitPdfToImages(file) {
    const arrayBuffer = await file.arrayBuffer();
    const pdf = await pdfjsLib.getDocument({ data: arrayBuffer }).promise;
    const totalPages = pdf.numPages;
    const pages = [];

    showToast(`Splitting ${totalPages}-page PDF into images...`, 'indigo');

    // Reuse a single canvas to avoid GC pressure on large PDFs
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');

    for (let i = 1; i <= totalPages; i++) {
        const page = await pdf.getPage(i);
        // Scale 2.2 (~160 DPI) — denser than 2.0 for tiny medicine names,
        // batch numbers, and expiry dates that Gemini must read precisely
        const viewport = page.getViewport({ scale: 2.2 });
        canvas.width = viewport.width;
        canvas.height = viewport.height;

        await page.render({ canvasContext: ctx, viewport }).promise;

        // 0.90 JPEG — balances sharp text edges against file size
        const blob = await new Promise(resolve => canvas.toBlob(resolve, 'image/jpeg', 0.90));
        const fileName = `${file.name.replace(/\.pdf$/i, '')}_page_${String(i).padStart(3, '0')}.jpg`;

        pages.push({
            file: new File([blob], fileName, { type: 'image/jpeg' }),
            blob,
            type: 'image',
            // Metadata carried through to extractSingleImage for debugging
            _originalName: file.name,
            _totalPages: totalPages
        });

        // Progress every page for first 10, then every 5 to avoid UI thrash
        if (i <= 10 || i % 5 === 0 || i === totalPages) {
            updateBatchProgress(i, totalPages, `Split page ${i} of ${totalPages}...`);
            await new Promise(r => setTimeout(r, 0));
        }
        // Free canvas memory between pages
        ctx.clearRect(0, 0, canvas.width, canvas.height);
    }

    return pages;
}

// ═══════════════════════════════════════════════════════════════════
// 5a2. CLIENT-SIDE ZIP EXTRACTOR — Pull images from a ZIP archive
// ═══════════════════════════════════════════════════════════════════
async function extractZipToImages(file) {
    showToast('Extracting images from ZIP...', 'indigo');
    const zip = await JSZip.loadAsync(file);
    const imageExts = /\.(jpe?g|png|webp|gif|bmp)$/i;
    const entries = [];

    zip.forEach((relativePath, zipEntry) => {
        if (!zipEntry.dir && imageExts.test(relativePath)) {
            entries.push({ path: relativePath, entry: zipEntry });
        }
    });

    if (entries.length === 0) {
        showToast('No images found in ZIP', 'red');
        return [];
    }

    showToast(`Found ${entries.length} images in ZIP — extracting...`, 'indigo');
    const pages = [];

    for (let i = 0; i < entries.length; i++) {
        const { path, entry } = entries[i];
        const blob = await entry.async('blob');
        const ext = path.split('.').pop().toLowerCase();
        const mimeType = ext === 'png' ? 'image/png' : ext === 'webp' ? 'image/webp' : 'image/jpeg';
        const finalBlob = mimeType === 'image/jpeg' ? blob : await convertToJpeg(blob, mimeType);

        const fileName = path.split('/').pop();
        pages.push({
            file: new File([finalBlob], fileName, { type: 'image/jpeg' }),
            blob: finalBlob,
            type: 'image'
        });

        if ((i + 1) % 10 === 0 || i === entries.length - 1) {
            updateBatchProgress(i + 1, entries.length, `Extracting ${i + 1} of ${entries.length}...`);
            await new Promise(r => setTimeout(r, 0));
        }
    }

    return pages;
}

async function convertToJpeg(blob, mimeType) {
    const img = await loadImage(blob);
    const canvas = document.createElement('canvas');
    canvas.width = img.width; canvas.height = img.height;
    canvas.getContext('2d').drawImage(img, 0, 0);
    return new Promise(resolve => canvas.toBlob(resolve, 'image/jpeg', 0.92));
}

function loadImage(source) {
    return new Promise((resolve, reject) => {
        const img = new Image();
        img.onload = () => { URL.revokeObjectURL(img.src); resolve(img); };
        img.onerror = () => reject(new Error('Failed to load image'));
        img.src = URL.createObjectURL(source);
    });
}

// ═══════════════════════════════════════════════════════════════════
// 5b. BATCH QUEUE — Process multiple images with parallel workers
// ═══════════════════════════════════════════════════════════════════
function updateBatchProgress(current, total, status, detail) {
    const panel = $('#batch-progress-panel');
    panel.classList.remove('hidden');
    const pct = total > 0 ? Math.round((current / total) * 100) : 0;
    $('#batch-progress-count').textContent = `${current}/${total}`;
    $('#batch-progress-bar').style.width = `${pct}%`;
    $('#batch-progress-status').textContent = status;
    const detailEl = $('#batch-progress-detail');
    if (detail) { detailEl.textContent = detail; detailEl.classList.remove('hidden'); }
    else { detailEl.classList.add('hidden'); }
}

function setBatchPhase(phase) {
    const label = $('#batch-progress-label');
    const dot = $('#batch-progress-dot');
    const bar = $('#batch-progress-bar');
    if (phase === 'split') {
        label.textContent = 'Splitting PDF';
        dot.className = 'w-2 h-2 rounded-full bg-amber-500 animate-ping';
        bar.className = 'bg-amber-500 h-full transition-all duration-500';
    } else if (phase === 'quality') {
        label.textContent = 'Quality Check';
        dot.className = 'w-2 h-2 rounded-full bg-cyan-500 animate-ping';
        bar.className = 'bg-cyan-500 h-full transition-all duration-500';
    } else if (phase === 'extract') {
        label.textContent = 'Extracting via Gemini';
        dot.className = 'w-2 h-2 rounded-full bg-indigo-500 animate-ping';
        bar.className = 'bg-indigo-500 h-full transition-all duration-500';
    } else if (phase === 'merge') {
        label.textContent = 'Merging Invoices';
        dot.className = 'w-2 h-2 rounded-full bg-emerald-500 animate-ping';
        bar.className = 'bg-emerald-500 h-full transition-all duration-500';
    }
}

function hideBatchProgress() {
    $('#batch-progress-panel').classList.add('hidden');
}

// ═══════════════════════════════════════════════════════════════════
// 5b. MERGE CONTINUATION PAGES — Group consecutive pages sharing
//     the same distributor + invoice number into a single invoice
// ═══════════════════════════════════════════════════════════════════
function mergeInvoices(pages) {
    if (pages.length <= 1) return pages;

    // Normalize distributor name + invoice number for grouping
    function invoiceKey(p) {
        const dist = (p.distributor || '').trim().toLowerCase();
        const inv = (p.invoiceNumber || '').trim().toUpperCase();
        return `${dist}::${inv}`;
    }

    // Group pages in order — consecutive pages with the same key are continuations
    const merged = [];
    let current = null;

    for (const page of pages) {
        const key = invoiceKey(page);
        const isSameInvoice = current && key === current._key && key !== '::';

        if (isSameInvoice) {
            // Append lines from this page to the current invoice
            const existingLines = current.lineItems || [];
            const newLines = (page.lineItems || []).map(line => ({ ...line, _fromPage: page._pageIndex }));
            current.lineItems = [...existingLines, ...newLines];

            // Track page range
            current._pages = current._pages || [];
            current._pages.push(page._pageIndex);

            // Keep the better-quality blob (first page usually has header)
            // but track all blobs for image preview
            if (!current._pageBlobs) current._pageBlobs = [];
            if (page._blob) current._pageBlobs.push({ pageIndex: page._pageIndex, blob: page._blob, file: page._file });

            // Use the first page as the primary file for review image
            // Use the last page's invoiceTotal if available (it usually has the final total)
            if (page.invoiceTotal) current.invoiceTotal = page.invoiceTotal;

            console.log(`[RxExpiry] Merged page ${page._pageIndex} into existing invoice (${key})`);
        } else {
            // New invoice — push current if any and start fresh
            if (current) merged.push(current._data || current);
            current = {
                _key: key,
                _data: page,
                _pages: page._pageIndex != null ? [page._pageIndex] : [],
                _pageBlobs: page._blob ? [{ pageIndex: page._pageIndex, blob: page._blob, file: page._file }] : []
            };
        }
    }
    if (current) merged.push(current._data || current);

    // Tag merged invoices with page count for UI
    for (const inv of merged) {
        if (!inv._pages) continue;
        inv._pageCount = inv._pages.length;
        // If we have multiple page blobs, use the first one as primary for display
        if (inv._pageBlobs && inv._pageBlobs.length > 1) {
            inv._blob = inv._pageBlobs[0].blob;
            inv._file = inv._pageBlobs[0].file;
        }
    }

    console.log(`[RxExpiry] mergeInvoices: ${pages.length} pages → ${merged.length} invoices`);
    return merged;
}

async function processNextInQueue() {
    const queue = State.imageQueue;
    const total = queue.length;
    if (total === 0) {
        hideBatchProgress();
        showToast('No invoices to process', 'red');
        return;
    }

    const CONCURRENCY = 3;
    const MAX_RETRIES = 2;
    let nextIdx = 0;
    let completed = 0;
    let failedPages = [];
    State.extractedQueue = [];

    setBatchPhase('extract');

    async function processPageWithRetry(idx, retries) {
        const item = queue[idx];
        const pageLabel = item.pageIndex != null ? `Page ${item.pageIndex + 1}/${total}` : `Item ${idx + 1}/${total}`;

        try {
            if (!item.blob) {
                item.blob = item.type === 'pdf'
                    ? item.file
                    : new Blob([await item.file.arrayBuffer()], { type: item.file.type });
            }
                const result = await extractSingleImage(item.file, item.type, item.batchId, item.pageIndex, {
                    totalPages: item._totalPages,
                    originalName: item._originalName
                });
            if (result && result.lineItems && result.lineItems.length > 0) {
                State.extractedQueue.push({
                    ...result,
                    _queueIndex: idx,
                    _file: item.file,
                    _blob: item.blob,
                    _type: item.type,
                    _batchId: item.batchId || null,
                    _pageIndex: item.pageIndex ?? null
                });
                console.log(`[RxExpiry] Queue[${idx}] extracted ${result.lineItems.length} items`);
            } else {
                console.warn(`[RxExpiry] Queue[${idx}] returned no items — skipping`);
            }
        } catch (e) {
            console.error(`[RxExpiry] Queue[${idx}] extraction failed:`, e);
            if (retries > 0) {
                console.warn(`[RxExpiry] Retrying Queue[${idx}]... (${retries} attempts left)`);
                await new Promise(r => setTimeout(r, 1000));
                return processPageWithRetry(idx, retries - 1);
            } else {
                failedPages.push(pageLabel);
                console.error(`[RxExpiry] Queue[${idx}] exhausted all retries`);
            }
        }
    }

    async function runWorker() {
        while (nextIdx < total) {
            const idx = nextIdx++;
            const item = queue[idx];
            const pageLabel = item.pageIndex != null ? `Page ${item.pageIndex + 1}/${total}` : `Item ${idx + 1}/${total}`;
            updateBatchProgress(
                completed, total,
                `Extracting ${pageLabel}...`,
                `${completed + failedPages.length}/${total} done · ${Math.min(CONCURRENCY, total - idx)} active`
            );

            await processPageWithRetry(idx, MAX_RETRIES);

            completed++;
            updateBatchProgress(
                completed, total,
                failedPages.length > 0
                    ? `Extracted ${completed}/${total} (${failedPages.length} failed)`
                    : `Extracted ${completed}/${total}`,
                `${completed}/${total} complete`
            );
        }
    }

    const workers = [];
    for (let w = 0; w < Math.min(CONCURRENCY, total); w++) {
        workers.push(runWorker());
    }
    await Promise.all(workers);

    // Phase 4: Merge continuation pages that share the same distributor+invoice
    if (State.extractedQueue.length > 1 && State.batchId) {
        setBatchPhase('merge');
        updateBatchProgress(State.extractedQueue.length, State.extractedQueue.length, 'Merging continuation pages...');
        State.extractedQueue = mergeInvoices(State.extractedQueue);
    }

    hideBatchProgress();
    State.currentQueueIndex = 0;

    if (failedPages.length > 0) {
        showToast(`${failedPages.length} page(s) failed after retries: ${failedPages.join(', ')}`, 'amber');
    }
    if (State.extractedQueue.length > 0) {
        showToast(`${State.extractedQueue.length} invoices extracted — reviewing...`, 'green');
        showNextReview();
    } else {
        showToast('No invoices could be extracted', 'red');
    }
}

async function extractSingleImage(file, type, batchId, pageIndex, meta) {
    if (!isFirebaseReady()) return null;

    const { ref: storageRef, uploadBytes, getDownloadURL } = State._fbStorage;
    const { httpsCallable } = State._fbFunctions;

    const rand = Math.random().toString(36).slice(2, 6);
    const fileId = batchId
        ? `${State.pharmacyId}/${batchId}/page${pageIndex ?? 0}_${Date.now()}_${rand}`
        : `${State.pharmacyId}_${Date.now()}_${rand}`;
    const fileRef = storageRef(State._storage, `temp/${fileId}`);
    await uploadBytes(fileRef, file, {
        contentType: file.type || 'image/jpeg',
        customMetadata: {
            batchId: batchId || '',
            pageIndex: pageIndex != null ? String(pageIndex) : '',
            totalDocumentPages: meta?.totalPages != null ? String(meta.totalPages) : '',
            originalFileName: meta?.originalName || file.name || ''
        }
    });
    const downloadURL = await getDownloadURL(fileRef);

    const extractFn = httpsCallable(State._functions, 'extractInvoice');
    const response = await extractFn({
        fileUrl: downloadURL,
        fileId,
        pharmacyId: State.pharmacyId,
        batchId: batchId || undefined,
        pageIndex: pageIndex != null ? pageIndex : undefined
    });

    return response.data;
}

function showNextReview() {
    const idx = State.currentQueueIndex;
    if (idx >= State.extractedQueue.length) {
        State.extractedQueue = [];
        State.imageQueue = [];
        State.currentQueueIndex = 0;
        State.batchId = null;
        showToast('All invoices reviewed!', 'green');
        return;
    }

    const result = State.extractedQueue[idx];
    State.extractedData = result;

    // Show batch indicator with multi-page info
    const indicator = $('#review-batch-indicator');
    indicator.classList.remove('hidden');
    let label = `Invoice ${idx + 1} of ${State.extractedQueue.length}`;
    if (result._pageCount > 1) {
        label += ` · ${result._pageCount} pages merged`;
    }
    if (result._batchId) {
        label += ` · Batch`;
    }
    indicator.textContent = label;

    renderReviewPanel(result);

    // Show the scanned image (first page for merged multi-page invoices)
    if (result._blob && result._type !== 'pdf') {
        $('#review-invoice-img').src = URL.createObjectURL(result._blob);
        $('#review-invoice-img').classList.remove('hidden');
        $('#review-invoice-pdf').classList.add('hidden');
    } else if (result._type === 'pdf' && result._file) {
        $('#review-invoice-pdf').src = URL.createObjectURL(result._file);
        $('#review-invoice-pdf').classList.remove('hidden');
        $('#review-invoice-img').classList.add('hidden');
    } else {
        $('#review-invoice-img').classList.add('hidden');
        $('#review-invoice-pdf').classList.add('hidden');
    }

    const panel = $('#extraction-review-panel');
    panel.classList.remove('hidden');
    showToast(`Reviewing invoice ${idx + 1} of ${State.extractedQueue.length}`, 'indigo');
}

$('#btn-cancel-scan')?.addEventListener('click', () => {
    $('#precheck-feedback-panel').classList.add('hidden');
});

// ═══════════════════════════════════════════════════════════════════
// 6. EXTRACT INVOICE — Upload to Storage → Call Cloud Function → Gemini
// ═══════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════
// 6a. CONTINUATION-PAGE DETECTION — After single-image extraction,
// check if this page continues a recent pending invoice
// ═══════════════════════════════════════════════════════════════════
async function checkForContinuationPage(newResult) {
    if (!isFirebaseReady() || !newResult) return null;

    const { collection, query, orderBy, limit, getDocs, Timestamp } = State._fbFirestore;
    const fifteenMinAgo = Timestamp.fromDate(new Date(Date.now() - 15 * 60 * 1000));

    try {
        // Query recent invoices from the last 15 minutes (all are confirmed/saved)
        const q = query(
            collection(State._db, `pharmacies/${State.pharmacyId}/invoices`),
            orderBy('capturedAt', 'desc'),
            limit(10)
        );
        const snap = await getDocs(q);
        if (snap.empty) return null;

        const newDist = (newResult.distributor || '').trim().toLowerCase();
        const newInv = (newResult.invoiceNumber || '').trim().toUpperCase();
        const newPageInfo = newResult.captureQuality?.pageInfo;

        for (const doc of snap.docs) {
            const existing = doc.data();
            // Only consider invoices from last 15 minutes
            const captured = existing.capturedAt?.toDate ? existing.capturedAt.toDate().getTime() : 0;
            if (captured < Date.now() - 15 * 60 * 1000) break; // Sorted desc, rest are older

            const existDist = (existing.distributor || '').trim().toLowerCase();
            const existInv = (existing.invoiceNumber || '').trim().toUpperCase();

            // Match signals
            const distMatch = newDist && existDist && newDist === existDist;
            const invMatch = newInv && existInv && newInv === existInv;
            const invMissing = !newInv || !existInv;

            if (distMatch && (invMatch || invMissing)) {
                const created = existing.capturedAt?.toDate
                    ? existing.capturedAt.toDate().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                    : 'recently';
                return {
                    existingId: doc.id,
                    existingData: { id: doc.id, ...existing },
                    distLabel: existing.distributor || 'Unknown',
                    invLabel: existing.invoiceNumber || 'N/A',
                    created,
                    lineCount: (existing.lineItems || []).length,
                    pageInfo: newPageInfo
                };
            }
        }
    } catch (e) {
        console.warn('[RxExpiry] Continuation check failed:', e);
    }
    return null;
}

// ═══════════════════════════════════════════════════════════════════
// 6b. MERGE INTO EXISTING INVOICE — Append new page's lineItems to
// an existing pending invoice in Firestore
// ═══════════════════════════════════════════════════════════════════
async function mergeIntoExistingInvoice(existingId, existingData, newResult) {
    if (!isFirebaseReady()) return;

    const { doc, updateDoc } = State._fbFirestore;

    const existingLines = existingData.lineItems || [];
    const newLines = (newResult.lineItems || []).map(item => ({
        ...item,
        _fromPage: newResult.captureQuality?.pageInfo?.current || existingLines.length
    }));
    const mergedLines = [...existingLines, ...newLines];

    // Build update: append new lineItems, update page count
    const updateData = {
        lineItems: mergedLines,
        _pageCount: (existingData._pageCount || 1) + 1,
        _lastUpdated: new Date().toISOString()
    };

    // If the new page has totals, update them
    // (totals are usually on the final page with the summary block)
    if (newResult.invoiceTotal) updateData.invoiceTotal = newResult.invoiceTotal;
    if (newResult.totalTaxableAmount) updateData.totalTaxableAmount = newResult.totalTaxableAmount;
    if (newResult.cgstTotal) updateData.cgstTotal = newResult.cgstTotal;
    if (newResult.sgstTotal) updateData.sgstTotal = newResult.sgstTotal;

    const invoiceRef = doc(State._db, `pharmacies/${State.pharmacyId}/invoices`, existingId);
    await updateDoc(invoiceRef, updateData);
    console.log(`[RxExpiry] Merged ${newLines.length} items into invoice ${existingId} (${mergedLines.length} total)`);
}

async function sendToExtractInvoice() {
    showToast('Uploading invoice...', 'indigo');

    // Compute image hash for duplicate detection (cheap, pre-Gemini)
    if (State.currentImageBlob && State.currentImageFile?.type?.startsWith('image/')) {
        State.currentImageHash = await computeImageHash(State.currentImageBlob);
        if (State.currentImageHash) {
            const dup = await checkImageDuplicate(State.currentImageHash);
            if (dup) {
                const created = dup.capturedAt?.toDate ? dup.capturedAt.toDate().toLocaleDateString() : 'recently';
                const proceed = confirm(`This looks like an invoice you already uploaded on ${created} (${dup.distributor || 'Unknown'} #${dup.invoiceNumber || 'N/A'}). Continue anyway?`);
                if (!proceed) {
                    closeReviewPanel();
                    return;
                }
            }
        }
    } else {
        State.currentImageHash = null;
    }

    // Open review panel with loading state
    const panel = $('#extraction-review-panel');
    panel.classList.remove('hidden');
    $('#review-form-container').classList.remove('hidden');
    $('#review-form-container').classList.add('md:flex');
    $('#review-line-items').innerHTML = `
        <div class="flex flex-col items-center justify-center py-12 space-y-3">
            <div class="spinner"></div>
            <span class="text-xs text-slate-400 font-semibold">Uploading & extracting via Gemini...</span>
            <span class="text-[10px] text-slate-500">This may take 5-15 seconds</span>
        </div>`;

    // Show the scanned image
    if (State.currentImageBlob) {
        $('#review-invoice-img').src = URL.createObjectURL(State.currentImageBlob);
        $('#review-invoice-img').classList.remove('hidden');
        $('#review-invoice-pdf').classList.add('hidden');
    } else if (State.currentImageFile?.type === 'application/pdf') {
        $('#review-invoice-pdf').src = URL.createObjectURL(State.currentImageFile);
        $('#review-invoice-pdf').classList.remove('hidden');
        $('#review-invoice-img').classList.add('hidden');
    }

    let result = null;

    // ── LIVE MODE: Upload to Storage → Call extractInvoice Cloud Function ──
    if (isFirebaseReady()) {
        try {
            const { ref: storageRef, uploadBytes, getDownloadURL } = State._fbStorage;
            const { httpsCallable } = State._fbFunctions;

            // Step A: Upload file to Firebase Storage (temp path)
            const fileId = `${State.pharmacyId}_${Date.now()}`;
            const fileRef = storageRef(State._storage, `temp/${fileId}`);
            showToast('Uploading to Storage...', 'indigo');
            await uploadBytes(fileRef, State.currentImageFile);
            const downloadURL = await getDownloadURL(fileRef);
            console.log('[RxExpiry] Uploaded to Storage:', downloadURL.substring(0, 60) + '...');

            // Step B: Call extractInvoice Cloud Function (synchronous — one request, one response)
            showToast('Calling Gemini extraction...', 'indigo');
            const extractFn = httpsCallable(State._functions, 'extractInvoice');
            const response = await extractFn({
                fileUrl: downloadURL,
                fileId: fileId,
                pharmacyId: State.pharmacyId
            });

            result = response.data;
            console.log('[RxExpiry] Extract result:', result);

        } catch (e) {
            console.error('[RxExpiry] Extraction failed:', e);
            closeReviewPanel();
            showToast('Cloud extraction failed: ' + (e.message || 'Unknown error'), 'red');
            failPrecheck('Extraction failed. Please check your connection and try again.');
            return;
        }
    } else {
        closeReviewPanel();
        showToast('Firebase not configured — cannot extract invoice', 'red');
        failPrecheck('Extraction service unavailable. Please ensure Firebase is set up.');
        return;
    }

    // ── Step 6: Check captureQuality.readable ──
    if (result.captureQuality && !result.captureQuality.readable) {
        closeReviewPanel();
        showToast(`Unreadable: ${result.captureQuality.issues?.join(', ')}`, 'red');
        failPrecheck(`Invoice not readable: ${result.captureQuality.issues?.join(', ')}. Please retake.`);
        return;
    }

    // ── Step 7: Check for continuation page (single-image uploads only) ──
    // Only runs when NOT in a batch/gallery group — just a lone photo upload
    if (!State.batchId && result && result.lineItems?.length > 0) {
        const continuation = await checkForContinuationPage(result);
        if (continuation) {
            const pageHint = continuation.pageInfo
                ? ` (Page ${continuation.pageInfo.current} of ${continuation.pageInfo.total})`
                : '';
            const merge = confirm(
                `This looks like it continues invoice #${continuation.invLabel} ` +
                `from ${continuation.distLabel} (${continuation.lineCount} items, uploaded ${continuation.created})${pageHint}.\n\n` +
                `Merge into that invoice?`
            );
            if (merge) {
                await mergeIntoExistingInvoice(continuation.existingId, continuation.existingData, result);
                closeReviewPanel();
                showToast(`Merged into invoice #${continuation.invLabel}`, 'green');
                return;
            }
        }
    }

    // ── Step 8: Show Review screen ──
    State.extractedData = result;
    renderReviewPanel(result);
    showToast('Extraction complete — verify below', 'green');
}

// ═══════════════════════════════════════════════════════════════════
// 7. REVIEW PANEL (Step 7 from prompt)
// ═══════════════════════════════════════════════════════════════════
function bindReview() {
    $('#btn-review-reject').addEventListener('click', () => { closeReviewPanel(); showToast('Invoice discarded', 'amber'); });
    $('#btn-review-approve').addEventListener('click', saveConfirmedInvoice);
    $('#btn-toggle-to-form')?.addEventListener('click', () => {
        $('#review-visual-container').classList.add('hidden');
        $('#review-form-container').classList.remove('hidden');
        $('#review-form-container').classList.add('flex');
    });
    $('#btn-toggle-to-scan')?.addEventListener('click', () => {
        $('#review-visual-container').classList.remove('hidden');
        $('#review-form-container').classList.add('hidden');
    });
}

function closeReviewPanel() {
    $('#extraction-review-panel').classList.add('hidden');
    $('#review-batch-indicator').classList.add('hidden');
    State.extractedData = null; State.currentImageFile = null; State.currentImageBlob = null; State.currentImageHash = null;

    if (State.extractedQueue.length > 0 && State.currentQueueIndex < State.extractedQueue.length - 1) {
        State.currentQueueIndex++;
        setTimeout(() => showNextReview(), 300);
    } else if (State.extractedQueue.length > 0) {
        State.extractedQueue = [];
        State.imageQueue = [];
        State.currentQueueIndex = 0;
        State.batchId = null;
    }
}

function renderReviewPanel(data) {
    const distLabel = data.distributor || 'Unknown';
    const buyerLabel = data.buyerName ? ` → ${data.buyerName}` : '';
    let pageInfo = '';
    if (data.captureQuality?.pageInfo?.total) {
        pageInfo = ` · Page ${data.captureQuality.pageInfo.current || '?'} of ${data.captureQuality.pageInfo.total}`;
    }
    if (data._pageCount > 1) {
        pageInfo += ` · ${data._pageCount} pages merged`;
    }
    $('#review-distributor-lbl').textContent = `Distributor: ${distLabel}${buyerLabel}${pageInfo}`;
    const container = $('#review-line-items');
    const alerts = $('#pipeline-alerts');
    container.innerHTML = '';
    alerts.innerHTML = '';

    // Per-line validation alert banner
    const flaggedLines = (data.lineItems || []).filter(item => item.lineValidationFailed || item.columnShiftSuspected);
    if (flaggedLines.length > 0) {
        const hasColShift = flaggedLines.some(item => item.columnShiftSuspected);
        const alertClass = hasColShift ? 'bg-orange-500/10 border-orange-500/30 text-orange-300' : 'bg-rose-500/10 border-rose-500/30 text-rose-300';
        const labelClass = hasColShift ? 'text-orange-400' : 'text-rose-400';
        const itemClass = hasColShift ? 'text-orange-300/80' : 'text-rose-300/80';
        const hintClass = hasColShift ? 'text-orange-300/60' : 'text-rose-300/60';
        alerts.innerHTML = `<div class="${alertClass} border rounded-xl p-3 text-[10px] space-y-1">
            <span class="font-bold ${labelClass}">⚠ ${flaggedLines.length} line${flaggedLines.length > 1 ? 's' : ''} need${flaggedLines.length === 1 ? 's' : ''} attention:</span>
            <ul class="list-disc list-inside space-y-0.5 ${itemClass}">
                ${flaggedLines.map((item, i) => `<li>Line ${data.lineItems.indexOf(item) + 1}: ${esc(item.medicineName || 'Unknown')} — ${item.validationNote || 'Values don\'t match formula'}</li>`).join('')}
            </ul>
            <span class="${hintClass}">Verify these values against the original invoice. Fix or acknowledge to save.</span>
        </div>`;
    }

    (data.lineItems || []).forEach((item, i) => {
        const low = (item.confidence || 1) < 0.8;
        const flagged = item.lineValidationFailed;
        const colShift = item.columnShiftSuspected;
        const borderColor = colShift ? 'border-orange-500/60 bg-orange-500/5' : (flagged ? 'border-rose-500/60 bg-rose-500/5' : (low ? 'border-amber-500/40 bg-amber-500/5' : 'border-slate-700/50'));
        const div = document.createElement('div');
        div.className = `bg-slate-800/50 border ${borderColor} rounded-xl p-3 space-y-2 ${colShift ? 'line-item-validation-failed' : (flagged ? 'line-item-validation-failed' : (low ? 'line-item-low-confidence' : ''))}`;
        div.innerHTML = `
            <div class="flex justify-between items-start">
                <div class="flex-1 mr-2">
                    <input type="text" value="${esc(item.medicineName)}" data-field="medicineName" data-idx="${i}"
                        class="w-full bg-transparent font-heading font-bold text-slate-100 text-xs border-b border-transparent hover:border-slate-600 focus:border-indigo-500 focus:outline-none ${low ? 'confidence-low' : ''}">
                    ${item.mfac ? `<span class="text-[8px] text-slate-500">${esc(item.mfac)}</span>` : ''}
                </div>
                ${colShift ? '<span class="text-[8px] bg-orange-500/20 text-orange-400 px-1.5 py-0.5 rounded font-bold">COLUMN SHIFT</span>' : ''}
                ${flagged && !colShift ? '<span class="text-[8px] bg-rose-500/20 text-rose-400 px-1.5 py-0.5 rounded font-bold">ARITH MISMATCH</span>' : ''}
                ${low ? '<span class="text-[8px] bg-amber-500/20 text-amber-400 px-1.5 py-0.5 rounded font-bold">LOW CONF</span>' : ''}
            </div>
            <div class="grid grid-cols-3 gap-2 text-[10px]">
                <div class="space-y-0.5">
                    <label class="text-slate-500 font-bold">Batch</label>
                    <input type="text" value="${esc(item.batchNumber)}" data-field="batchNumber" data-idx="${i}"
                        class="w-full bg-slate-900/60 border border-slate-700 rounded px-2 py-1 font-mono text-slate-200 focus:outline-none focus:border-indigo-500 ${low ? 'confidence-low' : ''}">
                </div>
                <div class="space-y-0.5">
                    <label class="text-slate-500 font-bold">Expiry</label>
                    <input type="text" value="${esc(item.expiryDate)}" data-field="expiryDate" data-idx="${i}"
                        class="w-full bg-slate-900/60 border border-slate-700 rounded px-2 py-1 font-mono text-slate-200 focus:outline-none focus:border-indigo-500 ${low ? 'confidence-low' : ''}">
                </div>
                <div class="space-y-0.5">
                    <label class="text-slate-500 font-bold">Qty</label>
                    <input type="number" value="${item.quantityBilled}" data-field="quantityBilled" data-idx="${i}" min="0"
                        class="w-full bg-slate-900/60 border border-slate-700 rounded px-2 py-1 font-mono text-slate-200 focus:outline-none focus:border-indigo-500">
                </div>
            </div>
            <div class="grid grid-cols-4 gap-2 text-[10px]">
                <div class="space-y-0.5">
                    <label class="text-slate-500 font-bold">Trade ₹ (C.D. ${item.cdPercent||0}%)</label>
                    <input type="number" step="0.01" value="${item.tradePrice}" data-field="tradePrice" data-idx="${i}" min="0"
                        class="w-full bg-slate-900/60 border border-slate-700 rounded px-2 py-1 font-mono text-slate-200 focus:outline-none focus:border-indigo-500 line-total-input">
                </div>
                <div class="space-y-0.5">
                    <label class="text-slate-500 font-bold">Net ₹</label>
                    <input type="number" step="0.01" value="${item.netValue}" data-field="netValue" data-idx="${i}" min="0"
                        class="w-full bg-slate-900/60 border border-slate-700 rounded px-2 py-1 font-mono text-slate-200 focus:outline-none focus:border-indigo-500 line-total-input">
                </div>
                <div class="space-y-0.5">
                    <label class="text-slate-500 font-bold">GST ₹</label>
                    <input type="number" step="0.01" value="${item.gstValue}" data-field="gstValue" data-idx="${i}" min="0"
                        class="w-full bg-slate-900/60 border border-slate-700 rounded px-2 py-1 font-mono text-slate-200 focus:outline-none focus:border-indigo-500 line-total-input">
                </div>
                <div class="space-y-0.5">
                    <label class="text-slate-500 font-bold">Line Total</label>
                    <span class="line-total-display block w-full bg-slate-900/40 border border-slate-700/50 rounded px-2 py-1 font-mono text-slate-400 text-[10px]">${(item.lineTotal || (item.netValue + item.gstValue) || 0).toFixed(2)}</span>
                </div>
            </div>
            <div class="grid grid-cols-3 gap-2 text-[10px]">
                <div class="space-y-0.5">
                    <label class="text-slate-500 font-bold">Scm Disc ₹</label>
                    <input type="number" step="0.01" value="${item.scmDiscount || 0}" data-field="scmDiscount" data-idx="${i}" min="0"
                        class="w-full bg-slate-900/60 border border-slate-700 rounded px-2 py-1 font-mono text-slate-200 focus:outline-none focus:border-indigo-500">
                </div>
                <div class="space-y-0.5">
                    <label class="text-slate-500 font-bold">GST Rate</label>
                    <span class="block w-full bg-slate-900/40 border border-slate-700/50 rounded px-2 py-1 font-mono text-slate-400">${item.gstRate||0}%</span>
                </div>
                <div class="flex items-end justify-end">
                    <span class="text-[9px] ${colShift ? 'text-orange-400' : (flagged ? 'text-rose-400' : (low ? 'text-amber-400' : 'text-emerald-400'))}">${colShift ? (item.validationNote || 'Column shift detected') : (flagged ? (item.validationNote || 'Check values') : `Conf: ${((item.confidence||0)*100).toFixed(0)}%`)}</span>
                </div>
            </div>`;
        container.appendChild(div);
    });

    container.querySelectorAll('.line-total-input').forEach(inp => inp.addEventListener('input', () => window.triggerRecalculate()));
    $('#review-declared-total-input').value = data.invoiceTotal || 0;
    $('#review-scheme-discount-input').value = data.schemeDiscount || 0;
    $('#review-cash-discount-input').value = data.cashDiscount || 0;
    $('#review-roundoff-input').value = data.roundOff || 0;

    // CGST / SGST and Taxable summary
    const cgstEl = $('#review-cgst-val');
    const sgstEl = $('#review-sgst-val');
    const taxableEl = $('#review-taxable-val');
    if (cgstEl) cgstEl.textContent = `₹${(data.cgstTotal || 0).toFixed(2)}`;
    if (sgstEl) sgstEl.textContent = `₹${(data.sgstTotal || 0).toFixed(2)}`;
    if (taxableEl) taxableEl.textContent = `₹${(data.totalTaxableAmount || 0).toFixed(2)}`;

    // Computed Summary (from reconcileAndCalculateInvoice on server)
    const cs = data.computedSummary;
    const csPanel = $('#computed-summary-panel');
    if (cs && cs.totalItemsCount > 0) {
        csPanel.classList.remove('hidden');
        $('#computed-items-count').textContent = cs.totalItemsCount;
        $('#computed-total-qty').textContent = cs.totalQty;
        $('#computed-total-gst').textContent = `₹${(cs.totalGst || 0).toFixed(2)}`;
        $('#computed-grand-total').textContent = `₹${(cs.grandTotalComputed || 0).toFixed(2)}`;
        $('#computed-declared-total').textContent = `₹${(cs.grandTotalDeclared || 0).toFixed(2)}`;
        const badge = $('#computed-summary-badge');
        const discRow = $('#computed-discrepancy-row');
        const discVal = $('#computed-discrepancy-val');
        const disc = Math.abs(cs.discrepancy || 0);
        if (cs.grandTotalDeclared === 0) {
            badge.textContent = 'No Declared Total';
            badge.className = 'px-2 py-0.5 text-[9px] rounded font-bold uppercase tracking-wider bg-slate-800 text-slate-400';
            discRow.classList.add('hidden');
        } else if (disc <= 10) {
            badge.textContent = 'Matched';
            badge.className = 'px-2 py-0.5 text-[9px] rounded font-bold uppercase tracking-wider bg-emerald-500/15 text-emerald-400 border border-emerald-500/30';
            discRow.classList.add('hidden');
        } else {
            badge.textContent = `Diff ₹${disc.toFixed(2)}`;
            badge.className = 'px-2 py-0.5 text-[9px] rounded font-bold uppercase tracking-wider bg-rose-500/15 text-rose-400 border border-rose-500/30';
            discRow.classList.remove('hidden');
            discVal.textContent = `₹${cs.discrepancy > 0 ? '+' : ''}${cs.discrepancy.toFixed(2)}`;
            discVal.className = `font-mono font-bold ${disc > 10 ? 'text-rose-400' : 'text-amber-400'}`;
        }
    } else {
        csPanel.classList.add('hidden');
    }

    window.triggerRecalculate();
}

window.triggerRecalculate = function () {
    const data = State.extractedData;
    if (!data) return;

    $$('#review-line-items input[data-field]').forEach(inp => {
        const idx = parseInt(inp.dataset.idx);
        if (data.lineItems[idx]) {
            data.lineItems[idx][inp.dataset.field] = inp.type === 'number' ? parseFloat(inp.value)||0 : inp.value;
        }
    });

    // Preserve OCR-extracted netValue/gstValue as the source of truth.
    // Only compute from formula as fallback when extracted value is missing/zero.
    data.lineItems.forEach(m => {
        if ((!m.netValue || m.netValue === 0) && m.tradePrice > 0 && m.quantityBilled > 0) {
            const cdMultiplier = 1 - ((m.cdPercent || 0) / 100);
            m.netValue = +(m.tradePrice * m.quantityBilled * cdMultiplier).toFixed(2);
        }
        if ((!m.gstValue || m.gstValue === 0) && m.netValue > 0 && m.gstRate > 0) {
            m.gstValue = +(m.netValue * m.gstRate / 100).toFixed(2);
        }
        m.lineTotal = +((m.netValue || 0) + (m.gstValue || 0)).toFixed(2);
    });

    let sumNet = 0, sumGst = 0;
    data.lineItems.forEach((m, i) => { sumNet += +m.netValue||0; sumGst += +m.gstValue||0; });
    // Update line total, net, gst displays in DOM
    $$('#review-line-items input[data-field="netValue"]').forEach((inp, i) => {
        if (data.lineItems[i]) inp.value = data.lineItems[i].netValue;
    });
    $$('#review-line-items input[data-field="gstValue"]').forEach((inp, i) => {
        if (data.lineItems[i]) inp.value = data.lineItems[i].gstValue;
    });
    $$('#review-line-items .line-total-display').forEach((el, i) => {
        if (data.lineItems[i]) el.textContent = (data.lineItems[i].lineTotal || 0).toFixed(2);
    });
    const schemeDiscount = parseFloat($('#review-scheme-discount-input')?.value) || 0;
    const cashDiscount = parseFloat($('#review-cash-discount-input')?.value) || 0;
    const roundOff = parseFloat($('#review-roundoff-input')?.value) || 0;
    const computed = sumNet + sumGst - schemeDiscount + roundOff;
    const declared = parseFloat($('#review-declared-total-input').value) || 0;

    $('#review-subtotal-val').textContent = `₹${sumNet.toFixed(2)}`;
    $('#review-gst-val').textContent = `₹${sumGst.toFixed(2)}`;

    const diff = Math.abs(computed - declared);
    const ok = diff <= 10 || declared === 0;
    const badge = $('#review-arithmetic-badge');
    const warn = $('#arithmetic-warning-banner');
    const ack = $('#arithmetic-ack-container');

    if (declared === 0) {
        badge.textContent = 'No Total'; badge.className = 'px-2 py-0.5 text-[9px] rounded font-bold uppercase tracking-wider bg-slate-800 text-slate-400';
        warn.classList.add('hidden'); ack.classList.add('hidden'); ack.classList.remove('flex');
    } else if (ok) {
        badge.textContent = 'Arithmetic OK'; badge.className = 'px-2 py-0.5 text-[9px] rounded font-bold uppercase tracking-wider bg-emerald-500/15 text-emerald-400 border border-emerald-500/30';
        warn.classList.add('hidden'); ack.classList.add('hidden'); ack.classList.remove('flex');
    } else {
        badge.textContent = `Mismatch ₹${diff.toFixed(2)}`; badge.className = 'px-2 py-0.5 text-[9px] rounded font-bold uppercase tracking-wider bg-rose-500/15 text-rose-400 border border-rose-500/30';
        warn.classList.remove('hidden');
        warn.innerHTML = `<span class="font-bold">Totals don't match!</span> Computed ₹${computed.toFixed(2)} (Net + GST − Discount + Rounding) vs Declared ₹${declared.toFixed(2)} (diff ₹${diff.toFixed(2)}).`;
        ack.classList.remove('hidden'); ack.classList.add('flex');
    }
};

// ═══════════════════════════════════════════════════════════════════
// 8. CONFIRM & SAVE (Step 8: Write Firestore → Delete temp file)
// ═══════════════════════════════════════════════════════════════════
async function saveConfirmedInvoice() {
    const data = State.extractedData;
    if (!data) return;

    // Per-line validation warnings are informational only — never block save.
    // The user has reviewed the extracted values and can fix them inline.

    // ── Data-level duplicate check (distributorId + invoiceNumber) ──
    if (data.distributor && data.invoiceNumber) {
        const dup = await checkInvoiceDuplicate(data.distributor, data.invoiceNumber);
        if (dup) {
            const created = dup.capturedAt?.toDate ? dup.capturedAt.toDate().toLocaleDateString() : 'previously';
            const proceed = confirm(`Invoice "${data.invoiceNumber}" from "${data.distributor}" was already recorded on ${created}. Save anyway?`);
            if (!proceed) {
                showToast('Duplicate invoice — save cancelled', 'amber');
                return;
            }
        }
    }

    const btn = $('#btn-review-approve');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner spinner-white inline-block"></span> Saving...';

    // Diagnostic: confirm auth state before attempting write
    const authReady = isFirebaseReady();
    const authUser = authReady ? State._auth.currentUser : null;
    console.log('[RxExpiry] Save auth check:', {
        firebaseReady: authReady,
        currentUser: authUser ? { uid: authUser.uid, isAnonymous: authUser.isAnonymous, phoneNumber: authUser.phoneNumber } : null,
        isUserAuthenticated: isUserAuthenticated()
    });

    if (!authReady || !authUser) {
        console.warn('[RxExpiry] Not authenticated — falling back to demo save');
        showToast('Not authenticated — saving locally', 'amber');
    }

    if (isUserAuthenticated()) {
        try {
            const { httpsCallable } = State._fbFunctions;
            const pharmacyId = State.pharmacyId;

            console.log('[RxExpiry] Saving via Cloud Function:', { pharmacyId, medicineCount: data.lineItems.length });

            const saveFn = httpsCallable(State._functions, 'saveInvoice');
            const result = await saveFn({
                pharmacyId: pharmacyId,
                invoice: {
                    distributor: data.distributor || '',
                    buyerName: data.buyerName || null,
                    invoiceNumber: data.invoiceNumber || '',
                    invoiceTotal: parseFloat($('#review-declared-total-input').value) || 0,
                    totalTaxableAmount: data.totalTaxableAmount || 0,
                    cgstTotal: data.cgstTotal || 0,
                    sgstTotal: data.sgstTotal || 0,
                    schemeDiscount: parseFloat($('#review-scheme-discount-input')?.value) || 0,
                    cashDiscount: parseFloat($('#review-cash-discount-input')?.value) || 0,
                    roundOff: parseFloat($('#review-roundoff-input')?.value) || 0,
                    pendingInvoicesCount: data.pendingInvoicesCount || 0,
                    pendingTotalAmount: data.pendingTotalAmount || 0,
                    imageHash: State.currentImageHash || null
                },
                medicines: data.lineItems.filter(item => item.medicineName && item.medicineName !== 'Could not parse - verify manually').map(item => ({
                    medicineName: item.medicineName,
                    batchNumber: item.batchNumber || '',
                    expiryDate: item.expiryDate || '',
                    quantityBilled: parseInt(item.quantityBilled) || 0,
                    quantityFree: parseInt(item.quantityFree) || 0,
                    tradePrice: parseFloat(item.tradePrice) || 0,
                    cdPercent: parseFloat(item.cdPercent) || 0,
                    scmDiscount: parseFloat(item.scmDiscount) || 0,
                    netValue: parseFloat(item.netValue) || 0,
                    gstRate: parseFloat(item.gstRate) || 0,
                    gstValue: parseFloat(item.gstValue) || 0,
                    lineTotal: parseFloat(item.lineTotal) || 0,
                    mrp: parseFloat(item.mrp) || 0,
                    packSize: item.packSize || '',
                    hsnCode: item.hsnCode || '',
                    rack: item.rack || '',
                    ptr: parseFloat(item.ptr) || 0,
                    mfac: item.mfac || null,
                    confidence: item.confidence || 0
                })),
                tempFileId: data.fileId || null
            });

            console.log('[RxExpiry] Save result:', result.data);

            showToast(`Saved ${data.lineItems.length} items to Firestore!`, 'green');

            // Reset button before closing (needed for batch queue)
            btn.disabled = false;
            btn.textContent = 'Confirm & Save';

            closeReviewPanel();

            // Reload data from Firestore
            await fetchFirestoreData();

        } catch (e) {
            console.error('[RxExpiry] Save error:', e);
            console.error('[RxExpiry] Save error code:', e.code, 'message:', e.message, 'details:', e.details);
            showToast('Firestore save failed: ' + (e.message || e.code || 'Unknown error'), 'red');
            btn.disabled = false;
            btn.textContent = 'Confirm & Save';
            return;
        }
    } else {
        // Demo: save to local state
        await new Promise(r => setTimeout(r, 800));
        const invoiceId = `INV${Date.now().toString(36).toUpperCase()}`;
        data.lineItems.forEach(item => {
            if (!item.medicineName) return;
            State.medicines.unshift({
                id: `med_${Date.now()}_${Math.random().toString(36).slice(2,6)}`,
                medicineName: item.medicineName, batchNumber: item.batchNumber, expiryDate: item.expiryDate,
                quantityBilled: +item.quantityBilled||0, quantityFree: +item.quantityFree||0, remainingQty: +item.quantityBilled||0,
                unitPrice: +item.unitPrice||0, netValue: +item.netValue||0, gstRate: +item.gstRate||0, gstValue: +item.gstValue||0,
                distributor: data.distributor||'', invoiceId, confidence: item.confidence||0, soldToday: 0
            });
        });
        State.invoices.unshift({ id: invoiceId, distributor: data.distributor, invoiceNumber: data.invoiceNumber,
            invoiceTotal: parseFloat($('#review-declared-total-input').value)||0, lineItemCount: data.lineItems.length, imageHash: State.currentImageHash || null });
        closeReviewPanel();
        showToast(`Saved ${data.lineItems.length} items (demo)`, 'green');
    }

    renderExpiringList();
    updateStats();
}

// ═══════════════════════════════════════════════════════════════════
// 9. INVENTORY LIST + EXPIRING Batches
// ═══════════════════════════════════════════════════════════════════
function renderExpiringList() {
    const list = $('#expiring-list');
    const count = $('#expiring-count');
    const now = Date.now();
    const cut = new Date(now + 90*86400000);

    const expiring = State.medicines.filter(m => {
        if (!m.expiryDate) return false;
        const exp = parseExp(m.expiryDate);
        return exp && exp <= cut && (m.remainingQty||0) > 0;
    }).sort((a,b) => parseExp(a.expiryDate) - parseExp(b.expiryDate));

    count.textContent = `${expiring.length} record${expiring.length!==1?'s':''}`;

    if (!expiring.length) {
        list.innerHTML = '<div class="text-center py-6 text-xs text-slate-500">No expiring items. Record an invoice to populate.</div>';
        State.selectedMedIds.clear();
        updateBulkActionBar();
        return;
    }

    // ── Group by distributor ──
    const distGroups = {};
    expiring.forEach(m => {
        const name = m.distributor || 'Unknown';
        if (!distGroups[name]) distGroups[name] = { name, items: [] };
        distGroups[name].items.push(m);
    });

    // Enrich groups with distributor data (phone, returnWindowDays)
    State.distributors.forEach(d => {
        if (distGroups[d.name]) {
            distGroups[d.name].phone = d.phone || '';
            distGroups[d.name].returnWindowDays = d.returnWindowDays || 0;
        }
    });

    const sortedGroups = Object.values(distGroups).sort((a, b) => b.items.length - a.items.length);

    // Bulk action toolbar
    const expiredCount = expiring.filter(m => { const e = parseExp(m.expiryDate); return e && e <= new Date(now); }).length;
    const selectedCount = State.selectedMedIds.size;

    let toolbarHtml = '';
    if (State.selectMode) {
        toolbarHtml = `
        <div class="bg-slate-800/60 border border-indigo-500/30 rounded-xl p-3 flex flex-wrap items-center gap-2 mb-2">
            <span class="text-[10px] text-indigo-400 font-bold">${selectedCount} selected</span>
            <div class="flex-1"></div>
            <button onclick="selectAllExpiring()" class="text-[10px] px-2 py-1 rounded-md bg-slate-700 text-slate-300 font-bold hover:bg-slate-600 transition-all">Select All</button>
            ${expiredCount > 0 ? `<button onclick="selectExpiredOnly()" class="text-[10px] px-2 py-1 rounded-md bg-amber-600/20 text-amber-400 font-bold hover:bg-amber-600/30 transition-all">Select Expired (${expiredCount})</button>` : ''}
            <button onclick="clearSelection()" class="text-[10px] px-2 py-1 rounded-md bg-slate-700 text-slate-400 font-bold hover:bg-slate-600 transition-all">Clear</button>
            <button onclick="exitSelectMode()" class="text-[10px] px-2 py-1 rounded-md bg-slate-700 text-slate-400 font-bold hover:bg-slate-600 transition-all">Cancel</button>
        </div>`;
    } else {
        toolbarHtml = `
        <div class="flex items-center justify-between mb-1">
            <button onclick="enterSelectMode()" class="text-[10px] px-2.5 py-1 rounded-md bg-slate-800 border border-slate-700 text-slate-400 font-bold hover:text-indigo-400 hover:border-indigo-500/50 transition-all flex items-center gap-1">
                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"/></svg>
                Select
            </button>
        </div>`;
    }

    // ── Render grouped list ──
    let html = toolbarHtml;
    sortedGroups.forEach((group, gi) => {
        const phoneHtml = group.phone ? `<a href="tel:${group.phone.replace(/[^0-9+]/g, '')}" class="text-[10px] px-2 py-0.5 rounded bg-emerald-600/20 text-emerald-400 font-bold hover:bg-emerald-600/30 transition-all shrink-0">Call</a>` : '';
        const phoneNum = group.phone ? `<span class="text-[9px] text-slate-500 font-mono">${esc(group.phone)}</span>` : '';

        // Return window countdown
        let returnWindowHtml = '';
        if (group.returnWindowDays > 0) {
            const earliest = Math.min(...group.items.map(m => parseExp(m.expiryDate).getTime()));
            const windowClose = earliest - (group.returnWindowDays * 86400000);
            const daysLeft = Math.ceil((windowClose - now) / 86400000);
            if (daysLeft > 0) {
                returnWindowHtml = `<span class="text-[9px] px-1.5 py-0.5 rounded bg-sky-600/20 text-sky-400 font-bold">Return window: ${daysLeft}d left</span>`;
            } else {
                returnWindowHtml = `<span class="text-[9px] px-1.5 py-0.5 rounded bg-rose-600/20 text-rose-400 font-bold">Return window closed</span>`;
            }
        }

        const groupId = `exp-group-${gi}`;
        html += `
        <div class="bg-slate-800/40 border border-slate-700/30 rounded-xl overflow-hidden mb-2">
            <button onclick="toggleExpiringGroup('${groupId}')" class="w-full p-3 flex items-center justify-between gap-2 text-left hover:bg-slate-800/60 transition-colors">
                <div class="flex-1 min-w-0">
                    <div class="flex items-center gap-2 flex-wrap">
                        <h4 class="font-heading font-bold text-slate-100 text-xs">${esc(group.name)}</h4>
                        ${phoneHtml}
                        ${phoneNum}
                    </div>
                    <div class="flex items-center gap-2 mt-0.5">
                        <span class="text-[10px] text-slate-400">${group.items.length} expiring item${group.items.length!==1?'s':''}</span>
                        ${returnWindowHtml}
                    </div>
                </div>
                <svg id="exp-chevron-${gi}" class="w-4 h-4 text-slate-500 transition-transform duration-200 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
            </button>
            <div id="${groupId}" class="hidden border-t border-slate-700/30">
                ${group.items.map(m => {
                    const days = Math.ceil((parseExp(m.expiryDate) - now) / 86400000);
                    let cls, txt;
                    if (days <= 0) { cls='badge-expired'; txt='EXPIRED'; }
                    else if (days <= 30) { cls='badge-expiring'; txt=`${days}d left`; }
                    else { cls='badge-safe'; txt=`${days}d left`; }

                    const isSelected = State.selectedMedIds.has(m.id);
                    const checkbox = State.selectMode ? `
                        <label class="shrink-0 cursor-pointer" onclick="event.stopPropagation()">
                            <input type="checkbox" ${isSelected ? 'checked' : ''} onchange="toggleMedSelect('${m.id}')" class="accent-indigo-500 w-4 h-4 rounded border-slate-600 bg-slate-800">
                        </label>` : '';

                    const rowCls = isSelected ? 'border-indigo-500/60 bg-indigo-500/10' : 'border-slate-700/50';

                    return `<div class="px-3 py-2.5 border-b ${rowCls} last:border-0 flex items-center justify-between gap-2 transition-all ${isSelected ? 'ring-1 ring-indigo-500/30' : ''}">
                        ${checkbox}
                        <div class="flex-1 min-w-0">
                            <p class="text-[10px] font-bold text-slate-200 truncate">${esc(m.medicineName)}</p>
                            <p class="text-[9px] text-slate-500 font-mono">Batch: ${esc(m.batchNumber||'N/A')} · ${m.remainingQty||0} pkts</p>
                        </div>
                        <span class="px-2 py-1 text-[9px] rounded font-bold shrink-0 ${cls}">${txt}</span>
                        ${State.selectMode ? '' : `<button onclick="deleteMedicine('${m.id}','${esc(m.medicineName)}')" class="shrink-0 p-1.5 rounded-lg text-slate-500 hover:text-rose-400 hover:bg-rose-500/10 transition-all" title="Remove">
                            <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>
                        </button>`}
                    </div>`;
                }).join('')}
            </div>
        </div>`;
    });

    list.innerHTML = html;
    updateBulkActionBar();
}

window.toggleExpiringGroup = function(id) {
    const el = document.getElementById(id);
    const idx = id.replace('exp-group-', '');
    const chevron = $(`#exp-chevron-${idx}`);
    const isOpen = !el.classList.contains('hidden');

    // Collapse all
    $$('.view-pane [id^="exp-group-"]').forEach(e => e.classList.add('hidden'));
    $$('.view-pane [id^="exp-chevron-"]').forEach(e => e.style.transform = '');

    if (!isOpen) {
        el.classList.remove('hidden');
        if (chevron) chevron.style.transform = 'rotate(180deg)';
    }
};

window.deleteMedicine = async function(id, name) {
    if (!confirm(`Remove "${name}"? This can't be undone.`)) return;

    if (isUserAuthenticated()) {
        try {
            const { httpsCallable } = State._fbFunctions;
            const fn = httpsCallable(State._functions, 'deleteMedicine');
            await fn({ pharmacyId: State.pharmacyId, medicineId: id });
        } catch (e) {
            console.error('[RxExpiry] Delete failed:', e);
            showToast('Delete failed: ' + e.message, 'red');
            return;
        }
    }

    State.medicines = State.medicines.filter(m => m.id !== id);
    renderExpiringList();
    updateStats();
    showToast(`Removed "${name}"`, 'green');
};

// ═══════════════════════════════════════════════════════════════════
// 9b. BULK SELECT + DELETE
// ═══════════════════════════════════════════════════════════════════
window.enterSelectMode = function() {
    State.selectMode = true;
    State.selectedMedIds.clear();
    renderExpiringList();
};

window.exitSelectMode = function() {
    State.selectMode = false;
    State.selectedMedIds.clear();
    renderExpiringList();
};

window.toggleMedSelect = function(id) {
    if (State.selectedMedIds.has(id)) {
        State.selectedMedIds.delete(id);
    } else {
        State.selectedMedIds.add(id);
    }
    renderExpiringList();
};

window.selectAllExpiring = function() {
    const now = Date.now();
    const cut = new Date(now + 90*86400000);
    State.medicines.forEach(m => {
        if (!m.expiryDate) return;
        const exp = parseExp(m.expiryDate);
        if (exp && exp <= cut && (m.remainingQty||0) > 0) {
            State.selectedMedIds.add(m.id);
        }
    });
    renderExpiringList();
};

window.selectExpiredOnly = function() {
    const now = Date.now();
    State.selectedMedIds.clear();
    State.medicines.forEach(m => {
        if (!m.expiryDate) return;
        const exp = parseExp(m.expiryDate);
        if (exp && exp <= now && (m.remainingQty||0) > 0) {
            State.selectedMedIds.add(m.id);
        }
    });
    renderExpiringList();
};

window.clearSelection = function() {
    State.selectedMedIds.clear();
    renderExpiringList();
};

window.bulkDeleteSelected = async function() {
    const ids = Array.from(State.selectedMedIds);
    if (!ids.length) return;
    if (!confirm(`Delete ${ids.length} selected medicine${ids.length>1?'s':''}? This can't be undone.`)) return;

    const btn = $('#btn-bulk-delete');
    if (btn) { btn.disabled = true; btn.textContent = 'Deleting...'; }

    if (isUserAuthenticated()) {
        try {
            const { httpsCallable } = State._fbFunctions;
            const fn = httpsCallable(State._functions, 'bulkDeleteMedicines');
            await fn({ pharmacyId: State.pharmacyId, medicineIds: ids });
        } catch (e) {
            console.error('[RxExpiry] Bulk delete failed:', e);
            showToast('Bulk delete failed: ' + e.message, 'red');
            if (btn) { btn.disabled = false; btn.textContent = 'Delete Selected'; }
            return;
        }
    }

    State.medicines = State.medicines.filter(m => !State.selectedMedIds.has(m.id));
    const count = State.selectedMedIds.size;
    State.selectedMedIds.clear();
    State.selectMode = false;
    renderExpiringList();
    updateStats();
    showToast(`Deleted ${count} medicine${count>1?'s':''}`, 'green');
};

function updateBulkActionBar() {
    const bar = $('#bulk-action-bar');
    if (!bar) return;
    const count = State.selectedMedIds.size;
    if (State.selectMode && count > 0) {
        bar.classList.remove('hidden');
        $('#bulk-count-text').textContent = `${count} item${count>1?'s':''} selected`;
    } else {
        bar.classList.add('hidden');
    }
}

function updateStats() {
    const cut = new Date(Date.now() + 90*86400000);
    $('#stat-total-batches').textContent = State.medicines.length;
    $('#stat-expiring-batches').textContent = State.medicines.filter(m => { const e=parseExp(m.expiryDate); return e&&e<=cut; }).length;
    $('#stat-total-packs').textContent = State.medicines.reduce((s,m)=>s+(m.remainingQty||0),0);
    $('#stat-sold-today').textContent = State.medicines.reduce((s,m)=>s+(m.soldToday||0),0);
}

function parseExp(str) {
    if (!str) return null;
    const sep = str.includes('/') ? '/' : '-';
    const p = str.split(sep).map(Number);
    if (p.length === 3) return p[0] > 1900 ? new Date(p[0], p[1]-1, p[2]) : new Date(p[2] > 1900 ? p[2] : 2000+p[2], p[1]-1, p[0]);
    if (p.length === 2) return new Date(p[1] > 1900 ? p[1] : 2000+p[1], p[0]-1, 1);
    return null;
}

// ═══════════════════════════════════════════════════════════════════
// 10. SEARCH + MANUAL ENTRY
// ═══════════════════════════════════════════════════════════════════
function bindSearch() {
    const input = $('#search-input');
    const list = $('#search-autocomplete-list');

    input.addEventListener('input', () => {
        const q = input.value.toLowerCase().trim();
        if (q.length < 2) { list.classList.add('hidden'); return; }
        const matches = State.medicines.filter(m =>
            m.medicineName?.toLowerCase().includes(q) || m.batchNumber?.toLowerCase().includes(q)
        ).slice(0, 8);
        if (!matches.length) {
            list.innerHTML = `<div class="autocomplete-item px-4 py-3 text-xs text-slate-400 cursor-pointer" data-action="new">+ Register new batch manually</div>`;
        } else {
            list.innerHTML = matches.map(m => `
                <div class="autocomplete-item px-4 py-3 cursor-pointer" data-id="${m.id}">
                    <div class="font-heading font-bold text-slate-100 text-xs">${esc(m.medicineName)}</div>
                    <div class="text-[10px] text-slate-400 font-mono">Batch: ${esc(m.batchNumber||'N/A')} · Exp: ${esc(m.expiryDate||'N/A')} · Stock: ${m.remainingQty||0}</div>
                </div>`).join('');
        }
        list.classList.remove('hidden');
        list.querySelectorAll('.autocomplete-item').forEach(item => {
            item.addEventListener('click', () => {
                list.classList.add('hidden');
                if (item.dataset.action==='new') { input.value=''; showNewBatchForm(); return; }
                const med = State.medicines.find(m=>m.id===item.dataset.id);
                if (med) selectBatch(med);
            });
        });
    });
    document.addEventListener('click', e => {
        if (!e.target.closest('#search-input') && !e.target.closest('#search-autocomplete-list')) list.classList.add('hidden');
    });

    $('#btn-search-decrement').addEventListener('click', decrementBatch);
    $('#btn-save-manual-batch').addEventListener('click', saveManualBatch);
}

function selectBatch(med) {
    State.selectedBatch = med;
    $('#search-result-panel').classList.remove('hidden');
    $('#new-batch-fallback-form').classList.add('hidden');
    $('#search-med-name').textContent = med.medicineName;
    $('#search-med-batch').textContent = `Batch: ${med.batchNumber||'N/A'}`;
    $('#search-med-original-qty').textContent = med.quantityBilled||0;
    $('#search-med-remaining-qty').textContent = med.remainingQty||0;
    const exp = parseExp(med.expiryDate);
    const badge = $('#search-med-expiry');
    if (exp) {
        const d = Math.ceil((exp-Date.now())/86400000);
        if (d<=0) { badge.textContent='EXPIRED'; badge.className='px-2 py-0.5 text-[9px] rounded font-bold badge-expired'; }
        else if (d<=30) { badge.textContent=`${d}d left`; badge.className='px-2 py-0.5 text-[9px] rounded font-bold badge-expiring'; }
        else { badge.textContent=`Exp: ${med.expiryDate}`; badge.className='px-2 py-0.5 text-[9px] rounded font-bold badge-safe'; }
    }
}

async function decrementBatch() {
    const med = State.selectedBatch;
    if (!med) return;
    if ((med.remainingQty||0)<=0) { showToast('Stock is zero','red'); return; }
    const newQty = med.remainingQty - 1;

    if (isFirebaseReady()) {
        try {
            const { doc, updateDoc } = State._fbFirestore;
            await updateDoc(doc(State._db, `pharmacies/${State.pharmacyId}/medicines/${med.id}`), { remainingQty: newQty });
        } catch (e) { console.warn('[RxExpiry] Firestore update skipped:', e); }
    }

    med.remainingQty = newQty;
    $('#search-med-remaining-qty').textContent = newQty;
    showToast(`${med.medicineName} → ${newQty} remaining`, 'indigo');
    renderExpiringList();
}

function showNewBatchForm() {
    $('#search-result-panel').classList.add('hidden');
    $('#new-batch-fallback-form').classList.remove('hidden');
    populateDistSelects();
}

async function saveManualBatch() {
    const name = $('#manual-med-name').value.trim();
    const batch = $('#manual-med-batch').value.trim();
    const expiry = $('#manual-med-expiry').value;
    const qty = parseInt($('#manual-med-qty').value)||0;
    const dist = $('#manual-med-distributor').value;
    if (!name||!batch||!expiry||qty<=0) { showToast('Fill all fields','red'); return; }

    const expFmt = expiry.split('-').reverse().join('/');

    if (isFirebaseReady()) {
        try {
            const { collection, addDoc, serverTimestamp } = State._fbFirestore;
            await addDoc(collection(State._db, `pharmacies/${State.pharmacyId}/medicines`), {
                medicineName: name, batchNumber: batch, expiryDate: expFmt,
                quantityBilled: qty, quantityFree: 0, remainingQty: qty, unitPrice: 0, netValue: 0,
                gstRate: 0, gstValue: 0, distributor: dist, invoiceId: null, confidence: 1, addedAt: serverTimestamp(), soldToday: 0
            });
        } catch (e) { console.warn('[RxExpiry] Firestore save skipped:', e); }
    }

    State.medicines.unshift({ id:`med_${Date.now()}`, medicineName:name, batchNumber:batch, expiryDate:expFmt,
        quantityBilled:qty, quantityFree:0, remainingQty:qty, unitPrice:0, netValue:0, gstRate:0, gstValue:0,
        distributor:dist, invoiceId:null, confidence:1, soldToday:0 });

    $('#manual-med-name').value=''; $('#manual-med-batch').value='';
    $('#manual-med-expiry').value=''; $('#manual-med-qty').value='';
    $('#new-batch-fallback-form').classList.add('hidden');
    showToast('Batch saved!','green');
    renderExpiringList();
}

// ═══════════════════════════════════════════════════════════════════
// 11. DISTRIBUTORS
// ═══════════════════════════════════════════════════════════════════
function renderDistributors() {
    const list = $('#distributors-list');
    
    // Build distributor map from medicines
    const distMap = {};
    State.medicines.forEach(m => {
        const name = m.distributor || 'Unknown';
        if (!distMap[name]) distMap[name] = { name, medicines: [], invoices: new Set() };
        distMap[name].medicines.push(m);
    });
    // Count invoices per distributor
    State.invoices.forEach(inv => {
        const name = inv.distributor || 'Unknown';
        if (distMap[name]) distMap[name].invoices.add(inv.id);
    });

    // Enrich with Firestore distributor data (phone, returnWindowDays)
    State.distributors.forEach(d => {
        if (distMap[d.name]) {
            distMap[d.name].phone = d.phone || '';
            distMap[d.name].returnWindowDays = d.returnWindowDays || 0;
            distMap[d.name].distId = d.id;
        }
    });

    const dists = Object.values(distMap).sort((a, b) => b.medicines.length - a.medicines.length);
    if (!dists.length) { list.innerHTML = '<div class="text-center py-8 text-xs text-slate-500">No distributors yet. Save an invoice to populate.</div>'; return; }

    list.innerHTML = dists.map((d, i) => {
        const invCount = d.invoices.size;
        const medCount = d.medicines.length;
        const now = Date.now();
        const cut90 = new Date(now + 90 * 86400000);
        const expiring = d.medicines.filter(m => { const e = parseExp(m.expiryDate); return e && e <= cut90; }).length;
        const phoneHtml = d.phone ? `<a href="tel:${d.phone.replace(/[^0-9+]/g, '')}" onclick="event.stopPropagation()" class="text-[10px] px-2 py-0.5 rounded bg-emerald-600/20 text-emerald-400 font-bold hover:bg-emerald-600/30 transition-all shrink-0">Call</a>` : '';
        return `<div class="distributor-card bg-slate-800/60 border border-slate-700/50 rounded-xl overflow-hidden">
            <button onclick="toggleDistributor(${i})" class="w-full p-3 flex items-center justify-between gap-3 text-left hover:bg-slate-800/40 transition-colors">
                <div class="flex-1 min-w-0">
                    <div class="flex items-center gap-2">
                        <h4 class="font-heading font-bold text-slate-100 text-xs">${esc(d.name)}</h4>
                        ${phoneHtml}
                        ${d.phone ? `<span class="text-[9px] text-slate-500 font-mono">${esc(d.phone)}</span>` : ''}
                    </div>
                    <p class="text-[10px] text-slate-400">${medCount} medicine${medCount !== 1 ? 's' : ''} · ${invCount} invoice${invCount !== 1 ? 's' : ''}${expiring ? ` · <span class="text-amber-400">${expiring} expiring</span>` : ''}</p>
                </div>
                <div class="flex items-center gap-2 shrink-0">
                    ${d.distId ? `<button onclick="event.stopPropagation(); editDistributor('${d.distId}')" class="p-1 rounded text-slate-500 hover:text-indigo-400 transition-all" title="Edit"><svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"/></svg></button>` : ''}
                    <svg id="dist-chevron-${i}" class="w-4 h-4 text-slate-500 transition-transform duration-200 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
                </div>
            </button>
            <div id="dist-expand-${i}" class="hidden border-t border-slate-700/50 max-h-64 overflow-y-auto">
                ${d.medicines.map(m => {
                    const exp = parseExp(m.expiryDate);
                    const days = exp ? Math.ceil((exp - now) / 86400000) : null;
                    let badge = '';
                    if (days !== null) {
                        if (days <= 0) badge = '<span class="text-[8px] px-1.5 py-0.5 rounded font-bold bg-rose-500/20 text-rose-400">EXPIRED</span>';
                        else if (days <= 30) badge = `<span class="text-[8px] px-1.5 py-0.5 rounded font-bold bg-amber-500/20 text-amber-400">${days}d</span>`;
                        else badge = `<span class="text-[8px] px-1.5 py-0.5 rounded font-bold bg-emerald-500/20 text-emerald-400">${days}d</span>`;
                    }
                    return `<div class="px-3 py-2 border-b border-slate-800/50 last:border-0 flex items-center justify-between gap-2">
                        <div class="min-w-0">
                            <p class="text-[10px] font-bold text-slate-200 truncate">${esc(m.medicineName)}</p>
                            <p class="text-[9px] text-slate-500 font-mono">Batch: ${esc(m.batchNumber||'N/A')} · Qty: ${m.remainingQty||0} · Exp: ${esc(m.expiryDate||'N/A')}</p>
                        </div>
                        ${badge}
                    </div>`;
                }).join('')}
            </div>
        </div>`;
    }).join('');
}

window.toggleDistributor = function(idx) {
    const expand = $(`#dist-expand-${idx}`);
    const chevron = $(`#dist-chevron-${idx}`);
    const isOpen = !expand.classList.contains('hidden');

    // Collapse all
    $$('.distributor-card [id^="dist-expand-"]').forEach(el => el.classList.add('hidden'));
    $$('.distributor-card [id^="dist-chevron-"]').forEach(el => el.style.transform = '');

    // If it was closed, open it (and rotate chevron)
    if (!isOpen) {
        expand.classList.remove('hidden');
        chevron.style.transform = 'rotate(180deg)';
    }
};

function populateDistSelects() {
    const sel = $('#manual-med-distributor');
    if (sel) sel.innerHTML = '<option value="">Select Distributor</option>' + State.distributors.map(d=>`<option value="${esc(d.name)}">${esc(d.name)}</option>`).join('');
}

function bindDistributorForm() {
    $('#btn-save-distributor').addEventListener('click', saveDistributor);
    $('#btn-cancel-dist-edit').addEventListener('click', () => {
        $('#dist-edit-id').value = '';
        $('#dist-name').value = '';
        $('#dist-phone').value = '';
        $('#dist-return-days').value = '';
        $('#distributor-form-title').textContent = 'Add Distributor';
        $('#btn-cancel-dist-edit').classList.add('hidden');
    });
}

async function saveDistributor() {
    const name = $('#dist-name').value.trim();
    const phone = $('#dist-phone').value.trim();
    const returnDays = parseInt($('#dist-return-days').value) || 0;
    if (!name) { showToast('Enter distributor name', 'red'); return; }

    const editId = $('#dist-edit-id').value || null;

    if (isFirebaseReady()) {
        try {
            const { httpsCallable } = State._fbFunctions;
            const fn = httpsCallable(State._functions, 'saveDistributor');
            await fn({ pharmacyId: State.pharmacyId, distributorId: editId, name, phone, returnWindowDays: returnDays });
        } catch (e) {
            console.error('[RxExpiry] Save distributor failed:', e);
            showToast('Save failed: ' + e.message, 'red');
            return;
        }
    }

    // Update local state
    if (editId) {
        const existing = State.distributors.find(d => d.id === editId);
        if (existing) { existing.name = name; existing.phone = phone; existing.returnWindowDays = returnDays; }
    } else {
        State.distributors.push({ id: editId || `dist_${Date.now()}`, name, phone, returnWindowDays: returnDays });
    }

    // Reset form
    $('#dist-edit-id').value = '';
    $('#dist-name').value = '';
    $('#dist-phone').value = '';
    $('#dist-return-days').value = '';
    $('#distributor-form-title').textContent = 'Add Distributor';
    $('#btn-cancel-dist-edit').classList.add('hidden');

    renderDistributors();
    populateDistSelects();
    showToast(editId ? 'Distributor updated' : 'Distributor added', 'green');
}

window.editDistributor = function(id) {
    const dist = State.distributors.find(d => d.id === id);
    if (!dist) return;
    $('#dist-edit-id').value = dist.id;
    $('#dist-name').value = dist.name || '';
    $('#dist-phone').value = dist.phone || '';
    $('#dist-return-days').value = dist.returnWindowDays || '';
    $('#distributor-form-title').textContent = 'Edit Distributor';
    $('#btn-cancel-dist-edit').classList.remove('hidden');
    $('#distributor-form').scrollIntoView({ behavior: 'smooth' });
};

// ═══════════════════════════════════════════════════════════════════
// 12. SETTINGS / ADMIN
// ═══════════════════════════════════════════════════════════════════
function bindSettings() {
    $('#btn-invite-staff').addEventListener('click', async () => {
        const phone = $('#invite-phone').value.trim();
        if (!phone||phone.length<10) { showToast('Enter valid phone','red'); return; }

        if (isFirebaseReady()) {
            try {
                const { collection, addDoc, serverTimestamp } = State._fbFirestore;
                await addDoc(collection(State._db, `pharmacies/${State.pharmacyId}/staff`), {
                    phone: `+91${phone}`, role: 'staff', invitedBy: State.user?.phone||'unknown', invitedAt: serverTimestamp(), active: true
                });
            } catch (e) { console.warn('[RxExpiry] Staff invite skipped:', e); }
        }

        State.staff.push({ phone: `+91${phone}`, role: 'staff' });
        showToast('Staff invite sent!','green');
        $('#invite-phone').value = '';
        loadStaffList();
    });
}

function loadStaffList() {
    const list = $('#active-staff-list');
    if (!State.staff.length) { list.innerHTML='<li class="text-slate-500 py-1">No staff yet</li>'; return; }
    list.innerHTML = State.staff.map(s=>`<li class="py-1 flex justify-between"><span>${esc(s.phone)}</span><span class="text-indigo-400">${s.role}</span></li>`).join('');
}

// ═══════════════════════════════════════════════════════════════════
// 13. THEME + EXPORT
// ═══════════════════════════════════════════════════════════════════
function bindThemeToggle() {
    $('#theme-toggle').addEventListener('click', () => {
        State.isDark = !State.isDark;
        document.body.classList.toggle('bg-slate-950', State.isDark);
        document.body.classList.toggle('text-slate-100', State.isDark);
        document.body.classList.toggle('bg-slate-50', !State.isDark);
        document.body.classList.toggle('text-slate-900', !State.isDark);
        showToast(State.isDark?'Dark mode':'Light mode','indigo');
    });
}

function bindExport() {
    $('#btn-export-csv')?.addEventListener('click', () => {
        if (!State.medicines.length) { showToast('No data','red'); return; }
        const h = ['Medicine','Batch','Expiry','Qty','Remaining','Unit₹','Net₹','GST₹','Distributor'];
        const csv = [h,...State.medicines.map(m=>[m.medicineName,m.batchNumber,m.expiryDate,m.quantityBilled,m.remainingQty,m.unitPrice,m.netValue,m.gstValue,m.distributor])].map(r=>r.map(c=>`"${(c||'').toString().replace(/"/g,'""')}"`).join(',')).join('\n');
        dl(csv,`rxexpiry_${Date.now()}.csv`,'text/csv'); showToast('CSV exported!','green');
    });
    $('#btn-export-json')?.addEventListener('click', () => {
        if (!State.medicines.length) { showToast('No data','red'); return; }
        dl(JSON.stringify(State.medicines,null,2),`rxexpiry_${Date.now()}.json`,'application/json'); showToast('JSON exported!','green');
    });
}

function dl(content, name, mime) {
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([content],{type:mime}));
    a.download = name; document.body.appendChild(a); a.click(); a.remove();
}

// ═══════════════════════════════════════════════════════════════════
// DEMO DATA
// ═══════════════════════════════════════════════════════════════════
function getDemoMedicines() {
    return [
        { id:'d1', medicineName:'Azithral 500mg Tablet', batchNumber:'AZ2214', expiryDate:'03/2026', quantityBilled:20, quantityFree:2, remainingQty:14, unitPrice:85.50, netValue:1710, gstRate:12, gstValue:205.20, distributor:'Medico Pharma', confidence:0.95, soldToday:2 },
        { id:'d2', medicineName:'Paracetamol 650mg Tab', batchNumber:'PCM1188', expiryDate:'09/2025', quantityBilled:50, quantityFree:5, remainingQty:38, unitPrice:12.00, netValue:600, gstRate:12, gstValue:72, distributor:'HealthLine Dist.', confidence:0.92, soldToday:5 },
        { id:'d3', medicineName:'Amoxicillin 250mg Cap', batchNumber:'AMX5501', expiryDate:'02/2026', quantityBilled:30, quantityFree:3, remainingQty:30, unitPrice:45.00, netValue:1350, gstRate:12, gstValue:162, distributor:'Medico Pharma', confidence:0.88, soldToday:0 },
        { id:'d4', medicineName:'Pan-D Pantoprazole Tab', batchNumber:'PAN9933', expiryDate:'11/2025', quantityBilled:40, quantityFree:4, remainingQty:22, unitPrice:120.00, netValue:4800, gstRate:12, gstValue:576, distributor:'Wellness Rx', confidence:0.97, soldToday:3 },
        { id:'d5', medicineName:'Cetirizine 10mg Tab', batchNumber:'CTZ7766', expiryDate:'08/2025', quantityBilled:60, quantityFree:6, remainingQty:55, unitPrice:8.50, netValue:510, gstRate:12, gstValue:61.20, distributor:'HealthLine Dist.', confidence:0.91, soldToday:1 },
        { id:'d6', medicineName:'Montair LC 10mg Tab', batchNumber:'MNL4421', expiryDate:'01/2026', quantityBilled:25, quantityFree:2, remainingQty:20, unitPrice:195.00, netValue:4875, gstRate:12, gstValue:585, distributor:'Wellness Rx', confidence:0.94, soldToday:0 },
        { id:'d7', medicineName:'Dolo 650mg Tablet', batchNumber:'DLO8899', expiryDate:'12/2025', quantityBilled:100, quantityFree:10, remainingQty:72, unitPrice:30.00, netValue:3000, gstRate:12, gstValue:360, distributor:'Medico Pharma', confidence:0.96, soldToday:8 },
        { id:'d8', medicineName:'Pantop 40mg Tablet', batchNumber:'PNT3344', expiryDate:'07/2025', quantityBilled:35, quantityFree:3, remainingQty:15, unitPrice:75.00, netValue:2625, gstRate:12, gstValue:315, distributor:'HealthLine Dist.', confidence:0.89, soldToday:4 },
    ];
}
function getDemoDistributors() {
    return [
        { id:'dist1', name:'Medico Pharma', contact:'+91 98765 43210', returnWindow:45, totalInvoices:24, activeBatches:12 },
        { id:'dist2', name:'HealthLine Dist.', contact:'+91 87654 32109', returnWindow:30, totalInvoices:18, activeBatches:8 },
        { id:'dist3', name:'Wellness Rx', contact:'+91 76543 21098', returnWindow:60, totalInvoices:31, activeBatches:15 },
    ];
}
function getDemoInvoices() {
    return [
        { id:'INV001', distributor:'Medico Pharma', invoiceNumber:'MC-2241', invoiceTotal:28165.40, lineItemCount:4 },
        { id:'INV002', distributor:'HealthLine Dist.', invoiceNumber:'HL-8812', invoiceTotal:8625.20, lineItemCount:3 },
    ];
}
function getDemoStaff() {
    return [{ phone:'+91 99999 11111', role:'staff' }];
}

function esc(str) { const d=document.createElement('div'); d.textContent=str||''; return d.innerHTML; }
