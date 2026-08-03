const fs = require('fs');
const path = require('path');

// Reuse existing extraction function — the same Gemini call path as
// test_extract.js (and production). Do not re-implement the API call here.
const { extractInvoiceFromImages } = require('./test_extract');

const TOLERANCE = 2; // ₹2, matches existing app tolerance

function findImagesForInvoice(invoiceNo) {
  const dir = path.join(__dirname, '../testdata/images');
  if (!fs.existsSync(dir)) return [];
  // match filenames containing the invoice number, sorted so page1 comes before page2
  return fs.readdirSync(dir)
    .filter(f => f.includes(invoiceNo.replace(/\s/g, '')))
    .sort()
    .map(f => path.join(dir, f));
}

async function runGroundTruthTests() {
  const groundTruthPath = path.join(__dirname, '../testdata/ground_truth_invoices.json');
  const { invoices } = JSON.parse(fs.readFileSync(groundTruthPath, 'utf8'));

  const results = [];

  for (const testCase of invoices) {
    const imagePaths = findImagesForInvoice(testCase.invoiceNo); // see Step 3
    if (!imagePaths || imagePaths.length === 0) {
      results.push({ invoiceNo: testCase.invoiceNo, status: 'SKIPPED', reason: 'no local image files found' });
      continue;
    }

    let extracted;
    try {
      extracted = await extractInvoiceFromImages(imagePaths); // must return merged { lineItems, invoiceSummary }
    } catch (err) {
      results.push({ invoiceNo: testCase.invoiceNo, status: 'ERROR', reason: err.message });
      continue;
    }

    const diffs = diffAgainstExpected(extracted, testCase.expected);
    results.push({
      invoiceNo: testCase.invoiceNo,
      status: diffs.length === 0 ? 'PASS' : 'FAIL',
      diffs,
      note: testCase.note || null
    });
  }

  printReport(results);
  return results;
}

function diffAgainstExpected(extracted, expected) {
  const diffs = [];

  if (expected.lineItemCount != null) {
    const actualCount = extracted.lineItems.length;
    if (actualCount !== expected.lineItemCount) {
      diffs.push(`lineItemCount: expected ${expected.lineItemCount}, got ${actualCount}`);
    }
  }

  const fields = ['saleValue', 'schDisc', 'cashDiscount', 'totalGst', 'roundOff', 'cnNo', 'grandTotal'];
  for (const field of fields) {
    const exp = expected.invoiceSummary[field];
    const act = extracted.invoiceSummary ? extracted.invoiceSummary[field] : undefined;
    if (exp == null) continue;
    if (act == null || Math.abs(act - exp) > TOLERANCE) {
      diffs.push(`${field}: expected ${exp}, got ${act}`);
    }
  }

  return diffs;
}

function printReport(results) {
  console.log('\n=== GROUND TRUTH REGRESSION TEST RESULTS ===\n');
  for (const r of results) {
    const icon = r.status === 'PASS' ? '✅' : r.status === 'SKIPPED' ? '⏭️' : '❌';
    console.log(`${icon} ${r.invoiceNo} — ${r.status}`);
    if (r.note) console.log(`   note: ${r.note}`);
    if (r.diffs && r.diffs.length) {
      r.diffs.forEach(d => console.log(`   - ${d}`));
    }
    if (r.reason) console.log(`   reason: ${r.reason}`);
  }
  const passed = results.filter(r => r.status === 'PASS').length;
  const failed = results.filter(r => r.status === 'FAIL').length;
  const skipped = results.filter(r => r.status === 'SKIPPED').length;
  console.log(`\n${passed} passed, ${failed} failed, ${skipped} skipped (of ${results.length})\n`);
}

module.exports = { runGroundTruthTests };

if (require.main === module) {
  runGroundTruthTests();
}
