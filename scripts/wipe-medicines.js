// scripts/wipe-medicines.js — RUN ONCE LOCALLY, DO NOT DEPLOY
// Requires: firebase-admin (cd functions && npm ls firebase-admin)
// Service account: download from Firebase Console → Project Settings → Service
//   Accounts → Generate New Private Key → save as scripts/serviceAccountKey.json
//
// Usage:
//   node scripts/wipe-medicines.js --list          → print all medicines + queue/return counts
//   node scripts/wipe-medicines.js --keep <docId>  → delete everything except <docId>
//
// What gets cleared:
//   • medicines — all docs except the one you --keep
//   • importQueue — all docs (transient staging data)
//   • pending_returns — all docs (transient staging data)
//
// If the --keep doc is part of a partial return (has returnedSplit siblings),
// keep BOTH halves or the data looks broken — pick a clean, never-returned
// record instead.

const admin = require('firebase-admin');

const PHARMACY_ID = 'city-pharma';   // change if targeting a different tenant
const BATCH_LIMIT = 450;            // stay under Firestore's 500-write limit

// ── Auth ──────────────────────────────────────────────────────────────────────
try {
  const keyPath = require('path').join(__dirname, 'serviceAccountKey.json');
  const serviceAccount = require(keyPath);
  admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
  console.log('Authenticated via serviceAccountKey.json');
} catch (_) {
  // Fall back to application-default (gcloud auth application-default login)
  admin.initializeApp({ credential: admin.credential.applicationDefault() });
  console.log('Authenticated via application-default credentials');
}

const db = admin.firestore();

// ── Helpers ───────────────────────────────────────────────────────────────────
async function listAll(collectionPath) {
  const snap = await db.collection(collectionPath).get();
  return snap.docs;
}

async function wipeCollection(collectionPath, keepIds = new Set()) {
  const snap = await db.collection(collectionPath).get();
  let batch = db.batch();
  let count = 0;

  for (const doc of snap.docs) {
    if (keepIds.has(doc.id)) {
      console.log(`  kept  ${doc.id}`);
      continue;
    }
    batch.delete(doc.ref);
    count++;
    if (count % BATCH_LIMIT === 0) {
      await batch.commit();
      console.log(`  … committed ${count} deletes so far`);
      batch = db.batch();
    }
  }
  await batch.commit();
  return count;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  const [,, cmd, arg] = process.argv;

  // ── LIST mode ──────────────────────────────────────────────────────────────
  if (cmd === '--list') {
    console.log(`\n=== Medicines (${PHARMACY_ID}) ===`);
    const meds = await listAll(`pharmacies/${PHARMACY_ID}/medicines`);
    if (meds.length === 0) { console.log('(empty)'); }
    for (const doc of meds) {
      const d = doc.data();
      const qty = d.returnedSplit
        ? `returnedQty=${d.returnedQty}`
        : `remaining=${d.remainingQty ?? d.quantityBilled}`;
      console.log(`  ${doc.id}  status=${d.status || 'active'}  ${qty}  ${d.medicineName || '?'}  batch=${d.batchNumber || '?'}`);
    }

    console.log(`\n=== importQueue ===`);
    const q = await listAll(`pharmacies/${PHARMACY_ID}/importQueue`);
    console.log(`  ${q.length} doc(s)`);

    console.log(`\n=== pending_returns ===`);
    const pr = await listAll(`pharmacies/${PHARMACY_ID}/pending_returns`);
    console.log(`  ${pr.length} doc(s)`);

    process.exit(0);
  }

  // ── WIPE mode ──────────────────────────────────────────────────────────────
  if (cmd === '--keep' && arg) {
    console.log(`\nWiping ${PHARMACY_ID}: keeping ${arg}, deleting everything else…\n`);

    const medCount = await wipeCollection(
      `pharmacies/${PHARMACY_ID}/medicines`,
      new Set([arg])
    );
    console.log(`\nMedicines: deleted ${medCount}\n`);

    const qCount = await wipeCollection(`pharmacies/${PHARMACY_ID}/importQueue`);
    console.log(`importQueue: deleted ${qCount}\n`);

    const prCount = await wipeCollection(`pharmacies/${PHARMACY_ID}/pending_returns`);
    console.log(`pending_returns: deleted ${prCount}\n`);

    console.log('Done. Check Firestore console to verify.');
    process.exit(0);
  }

  // ── Usage ──────────────────────────────────────────────────────────────────
  console.log('Usage:');
  console.log('  node scripts/wipe-medicines.js --list');
  console.log('  node scripts/wipe-medicines.js --keep <docId>');
  process.exit(1);
}

main().catch((err) => { console.error('FATAL:', err); process.exit(1); });
