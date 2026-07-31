const fs = require("fs");
const files = process.argv.slice(2);
const sum = (arr) => arr.reduce((a, b) => a + (Number(b) || 0), 0);
const r2 = (n) => Math.round(n * 100) / 100;

for (const f of files) {
  const j = JSON.parse(fs.readFileSync(f, "utf8"));
  const items = j.lineItems || [];
  const s = j.invoiceSummary || {};
  const sumCd = r2(sum(items.map((i) => i.cdValue)));
  const sumGst = r2(sum(items.map((i) => i.gstValue)));
  const sumTax = r2(sum(items.map((i) => i.taxableValue)));
  const sumNet = r2(sum(items.map((i) => i.netValue)));

  console.log("=".repeat(70));
  console.log(`${j.distributor} | ${j.invoiceNumber} | ${j.invoiceDate} | total=${j.invoiceTotal}`);
  console.log(`lines=${items.length} readable=${j.captureQuality?.readable} missingPage=${j.captureQuality?.missingPage}`);
  if (j.captureQuality?.issues?.length) console.log("issues:", j.captureQuality.issues.join("; "));

  console.log("-- GRAND TOTAL CHECK (footer formula) --");
  const sVal = s.saleValue || 0;
  const sch = s.schDisc || 0;
  const cash = s.cashDiscount || 0;
  const gst = s.totalGst || 0;
  const ro = s.roundOff || 0;
  const cn = s.cnNo || 0;
  const gtFooter = r2(sVal - sch - cash + gst + ro - cn);
  const gt1 = r2(sumTax + sumGst + ro);
  console.log(`  grandTotal(printed)=${s.grandTotal}`);
  console.log(`    saleValue-schDisc-cashDisc+gst+roundOff-cnNo = ${gtFooter}  ${gtFooter === s.grandTotal ? "MATCH" : "MISMATCH (Δ=" + r2(gtFooter - (s.grandTotal||0)) + ")"}`);
  console.log(`    sum(taxable)+sum(gst)+roundOff                = ${gt1}  ${gt1 === s.grandTotal ? "MATCH" : "MISMATCH (Δ=" + r2(gt1 - (s.grandTotal||0)) + ")"}`);

  console.log("-- CD/DISCOUNT CHECK (footer discount vs line taxables) --");
  const discFooter = r2(sch + cash);
  const discImplied = r2(sVal - sumTax);
  console.log(`  footer disc (schDisc+cashDiscount)=${discFooter}  saleValue−Σtaxable=${discImplied}  ${Math.abs(discFooter - discImplied) <= 2 ? "MATCH" : "MISMATCH (Δ=" + r2(discFooter - discImplied) + ")"}`);

  console.log("-- GST CHECK --");
  console.log(`  sum(line gstValue)=${sumGst}  printed totalGst=${s.totalGst}  ${r2(sumGst - (s.totalGst||0)) === 0 ? "MATCH" : "MISMATCH (Δ=" + r2(sumGst - (s.totalGst||0)) + ")"}`);
  if (s.totalCGST + s.totalSGST + s.totalIGST > 0)
    console.log(`  CGST+SGST+IGST printed sum=${r2(s.totalCGST + s.totalSGST + s.totalIGST)}`);

  console.log("-- PER-LINE GST RATE CHECK --");
  const rates = new Set();
  let rateIssues = 0;
  for (const i of items) {
    const tax = Number(i.taxableValue) || 0;
    const gv = Number(i.gstValue) || 0;
    const gr = Number(i.gstRate) || 0;
    if (gr) rates.add(gr);
    const expect = tax * gr / 100;
    if (gr && Math.abs(gv - expect) > 1) {
      rateIssues++;
      console.log(`  ⚠ ${(i.medicineName || "").slice(0, 24).padEnd(24)} gstRate=${gr}% taxable=${tax} → expected GST ₹${r2(expect)} but read ₹${gv}`);
    }
  }
  if (rateIssues === 0) console.log(`  all lines consistent. distinct rates: ${[...rates].sort((a, b) => a - b).join("%, ")}%`);

  console.log(`  sum(line netValue)=${sumNet} vs grandTotal=${s.grandTotal}`);
}
