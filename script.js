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
    extractedData: null,
    medicines: [],
    distributors: [],
    staff: [],
    invoices: [],
    selectedBatch: null,
    isDark: true,
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
        renderExpiringList();
        updateStats();
        console.log(`[RxExpiry] Loaded ${State.medicines.length} medicines, ${State.invoices.length} invoices from Firestore`);
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
    const file = e.target.files[0]; if (!file) return;
    State.currentImageFile = file;
    State.currentImageBlob = new Blob([await file.arrayBuffer()], { type: file.type });
    await runQualityCheck(State.currentImageBlob, 'image');
    e.target.value = '';
}

async function handlePdfUpload(e) {
    const file = e.target.files[0]; if (!file) return;
    State.currentImageFile = file;
    await runQualityCheck(file, 'pdf');
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
        if (file.size / 1048576 > 20) { failPrecheck('PDF too large (>20MB).'); return; }
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

function loadImage(file) {
    return new Promise((resolve, reject) => {
        const img = new Image();
        img.onload = () => { URL.revokeObjectURL(img.src); resolve(img); };
        img.onerror = reject;
        img.src = URL.createObjectURL(file);
    });
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

$('#btn-cancel-scan')?.addEventListener('click', () => {
    $('#precheck-feedback-panel').classList.add('hidden');
});

// ═══════════════════════════════════════════════════════════════════
// 6. EXTRACT INVOICE — Upload to Storage → Call Cloud Function → Gemini
// ═══════════════════════════════════════════════════════════════════
async function sendToExtractInvoice() {
    showToast('Uploading invoice...', 'indigo');

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

    // ── Step 7: Show Review screen ──
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
    State.extractedData = null; State.currentImageFile = null; State.currentImageBlob = null;
}

function renderReviewPanel(data) {
    $('#review-distributor-lbl').textContent = `Distributor: ${data.distributor || 'Unknown'}`;
    const container = $('#review-line-items');
    container.innerHTML = '';

    (data.lineItems || []).forEach((item, i) => {
        const low = (item.confidence || 1) < 0.8;
        const div = document.createElement('div');
        div.className = `bg-slate-800/50 border border-slate-700/50 rounded-xl p-3 space-y-2 ${low ? 'line-item-low-confidence' : ''}`;
        div.innerHTML = `
            <div class="flex justify-between items-start">
                <input type="text" value="${esc(item.medicineName)}" data-field="medicineName" data-idx="${i}"
                    class="bg-transparent font-heading font-bold text-slate-100 text-xs border-b border-transparent hover:border-slate-600 focus:border-indigo-500 focus:outline-none flex-1 mr-2 ${low ? 'confidence-low' : ''}">
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
            <div class="grid grid-cols-3 gap-2 text-[10px]">
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
            </div>
            <div class="flex justify-between items-center">
                <span class="text-[9px] text-slate-500">GST Rate: ${item.gstRate||0}%</span>
                <span class="text-[9px] ${low ? 'text-amber-400' : 'text-emerald-400'}">Conf: ${((item.confidence||0)*100).toFixed(0)}%</span>
            </div>`;
        container.appendChild(div);
    });

    container.querySelectorAll('.line-total-input').forEach(inp => inp.addEventListener('input', () => window.triggerRecalculate()));
    $('#review-declared-total-input').value = data.invoiceTotal || 0;
    $('#review-scheme-discount-input').value = data.schemeDiscount || 0;
    $('#review-cash-discount-input').value = data.cashDiscount || 0;
    $('#review-roundoff-input').value = data.roundOff || 0;
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

    // Recompute netValue from tradePrice × qty × (1 - cdPercent/100) if tradePrice or cdPercent changed
    data.lineItems.forEach(m => {
        if (m.tradePrice > 0 && m.quantityBilled > 0) {
            const cdMultiplier = 1 - ((m.cdPercent || 0) / 100);
            m.netValue = +(m.tradePrice * m.quantityBilled * cdMultiplier).toFixed(2);
        }
        if (m.netValue > 0 && m.gstRate > 0) {
            m.gstValue = +(m.netValue * m.gstRate / 100).toFixed(2);
        }
    });

    let sumNet = 0, sumGst = 0;
    data.lineItems.forEach(m => { sumNet += +m.netValue||0; sumGst += +m.gstValue||0; });
    const schemeDiscount = parseFloat($('#review-scheme-discount-input')?.value) || 0;
    const cashDiscount = parseFloat($('#review-cash-discount-input')?.value) || 0;
    const roundOff = parseFloat($('#review-roundoff-input')?.value) || 0;
    const computed = sumNet + sumGst - schemeDiscount + roundOff;
    const declared = parseFloat($('#review-declared-total-input').value) || 0;

    $('#review-subtotal-val').textContent = `₹${sumNet.toFixed(2)}`;
    $('#review-gst-val').textContent = `₹${sumGst.toFixed(2)}`;

    const diff = Math.abs(computed - declared);
    const ok = diff <= 2 || declared === 0;
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

    const hasMismatch = $('#review-arithmetic-badge').textContent.includes('Mismatch');
    if (hasMismatch && !$('#arithmetic-ack-checkbox').checked) {
        showToast('Acknowledge the mismatch first', 'red'); return;
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
                    invoiceNumber: data.invoiceNumber || '',
                    invoiceTotal: parseFloat($('#review-declared-total-input').value) || 0,
                    schemeDiscount: parseFloat($('#review-scheme-discount-input')?.value) || 0,
                    cashDiscount: parseFloat($('#review-cash-discount-input')?.value) || 0,
                    roundOff: parseFloat($('#review-roundoff-input')?.value) || 0
                },
                medicines: data.lineItems.filter(item => item.medicineName && item.medicineName !== 'Could not parse - verify manually').map(item => ({
                    medicineName: item.medicineName,
                    batchNumber: item.batchNumber || '',
                    expiryDate: item.expiryDate || '',
                    quantityBilled: parseInt(item.quantityBilled) || 0,
                    quantityFree: parseInt(item.quantityFree) || 0,
                    tradePrice: parseFloat(item.tradePrice) || 0,
                    cdPercent: parseFloat(item.cdPercent) || 0,
                    netValue: parseFloat(item.netValue) || 0,
                    gstRate: parseFloat(item.gstRate) || 0,
                    gstValue: parseFloat(item.gstValue) || 0,
                    confidence: item.confidence || 0
                })),
                tempFileId: data.fileId || null
            });

            console.log('[RxExpiry] Save result:', result.data);

            closeReviewPanel();
            showToast(`Saved ${data.lineItems.length} items to Firestore!`, 'green');

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
            invoiceTotal: parseFloat($('#review-declared-total-input').value)||0, lineItemCount: data.lineItems.length });
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
        return;
    }

    list.innerHTML = expiring.map(m => {
        const days = Math.ceil((parseExp(m.expiryDate) - now) / 86400000);
        let cls, txt;
        if (days <= 0) { cls='badge-expired'; txt='EXPIRED'; }
        else if (days <= 30) { cls='badge-expiring'; txt=`${days}d left`; }
        else { cls='badge-safe'; txt=`${days}d left`; }
        return `<div class="bg-slate-800/60 border border-slate-700/50 rounded-xl p-3 flex items-center justify-between gap-3">
            <div class="flex-1 min-w-0">
                <h4 class="font-heading font-bold text-slate-100 text-xs truncate">${esc(m.medicineName)}</h4>
                <p class="text-[10px] text-slate-400 font-mono">Batch: ${esc(m.batchNumber||'N/A')} · ${m.remainingQty||0} pkts · ${esc(m.distributor||'')}</p>
            </div>
            <span class="px-2 py-1 text-[9px] rounded font-bold shrink-0 ${cls}">${txt}</span>
        </div>`;
    }).join('');
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
    if (!State.distributors.length) { list.innerHTML='<div class="text-center py-8 text-xs text-slate-500">No distributors.</div>'; return; }
    list.innerHTML = State.distributors.map(d => `
        <div class="distributor-card bg-slate-800/60 border border-slate-700/50 rounded-xl p-4 space-y-2">
            <div class="flex justify-between items-start">
                <div>
                    <h4 class="font-heading font-bold text-slate-100 text-sm">${esc(d.name)}</h4>
                    <p class="text-[10px] text-slate-400">Contact: ${esc(d.contact||'N/A')}</p>
                </div>
                <span class="px-2 py-0.5 text-[9px] rounded font-bold bg-indigo-500/15 text-indigo-400 border border-indigo-500/30">${d.returnWindow||30}d return</span>
            </div>
            <div class="grid grid-cols-2 gap-2 text-[10px]">
                <div class="bg-slate-900/50 p-2 rounded text-center"><span class="text-slate-500 block">Invoices</span><span class="font-bold text-slate-200">${d.totalInvoices||0}</span></div>
                <div class="bg-slate-900/50 p-2 rounded text-center"><span class="text-slate-500 block">Active Batches</span><span class="font-bold text-indigo-400">${d.activeBatches||0}</span></div>
            </div>
        </div>`).join('');
}

function populateDistSelects() {
    const sel = $('#manual-med-distributor');
    if (sel) sel.innerHTML = '<option value="">Select Distributor</option>' + State.distributors.map(d=>`<option value="${esc(d.name)}">${esc(d.name)}</option>`).join('');
}

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
