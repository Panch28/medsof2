# RxExpiry — Setup Instructions

## Quick Start (Demo Mode)
Just open `index.html` in a browser. Everything works offline with demo data.

## Production Setup

### 1. Create Firebase Project
1. Go to [Firebase Console](https://console.firebase.google.com)
2. Create a new project (or use existing)
3. Enable: Authentication (Phone sign-in), Firestore, Storage, Functions

### 2. Update Firebase Config
Edit `script.js` line 10-17 and replace `YOUR_*` values:
```js
const FIREBASE_CONFIG = {
    apiKey: "AIza...",
    authDomain: "your-project.firebaseapp.com",
    projectId: "your-project-id",
    storageBucket: "your-project.appspot.com",
    messagingSenderId: "123456789",
    appId: "1:123456789:web:abc123"
};
```

### 3. Set Gemini API Key as Firebase Secret
```bash
firebase functions:secrets:set GEMINI_API_KEY
# Paste your key when prompted — it is stored encrypted, never in source code
```
The Cloud Function reads it at runtime via `defineSecret("GEMINI_API_KEY").value()`.

### 4. Deploy
Edit `.firebaserc` and replace `YOUR_PROJECT_ID` with your Firebase project ID.

### 5. Deploy
```bash
# Install function dependencies
cd functions && npm install && cd ..

# Login to Firebase
firebase login

# Deploy everything
firebase deploy

# Or deploy selectively
firebase deploy --only firestore:rules,storage:rules
firebase deploy --only functions
firebase deploy --only hosting
```

## Architecture

```
Frontend (index.html + style.css + script.js)
  │
  ├── Auth: Firebase Phone OTP
  ├── Capture: Camera API / File Input
  ├── Quality Check: Canvas blur + exposure (client-side, no AI)
  │
  ├── Upload: Firebase Storage → temp/{fileId}
  │
  ├── Extract: Cloud Function → Gemini 3 Flash Preview
  │             (synchronous HTTPS call, one request, one response)
  │
  ├── Review: Confidence highlighting, arithmetic check (₹2 tolerance)
  │
  └── Save: Firestore → /pharmacies/{id}/medicines + /invoices
            + Delete temp file from Storage

Backend (functions/index.js)
  ├── extractInvoice: Gemini 3 Flash extraction (the ONLY AI model)
  └── cleanupTempFiles: Scheduled daily — deletes files > 30 days old
```

## Cloud Functions
- `extractInvoice` — HTTPS Callable, takes fileUrl + pharmacyId, returns structured extraction
- `cleanupTempFiles` — Scheduled daily, deletes orphaned temp files older than 30 days

## Firestore Structure
```
/pharmacies/{pharmacyId}/
  ├── medicines/{medicineId}
  ├── invoices/{invoiceId}
  ├── staff/{staffId}
  └── distributors/{distId}
```
