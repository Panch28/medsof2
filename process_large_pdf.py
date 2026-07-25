"""
process_large_pdf.py — Split a large PDF and extract each page via Gemini.

Usage:
    python process_large_pdf.py <path_to_pdf> [--dpi 200] [--concurrency 3]

Requirements:
    pip install PyMuPDF requests

Setup:
    Set your Gemini API key:
      set GEMINI_API_KEY=your_key_here       (Windows)
      export GEMINI_API_KEY=your_key_here     (Mac/Linux)
    Or create a .env file with: GEMINI_API_KEY=your_key_here

What it does:
    1. Splits the 300MB PDF into individual JPEG page images (locally, no upload)
    2. Sends each page directly to Gemini for invoice extraction
    3. Saves all results to a JSON file
    4. Cleans up page images
"""

import os
import sys
import json
import time
import base64
import argparse
import concurrent.futures
import fitz  # PyMuPDF
import requests

# ─── Gemini Config ───────────────────────────────────────────────
GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

# ─── Extraction Prompt (exact copy from Cloud Function) ──────────
EXTRACTION_PROMPT = r"""You are an expert OCR and financial data extraction engine specialized in Indian Pharmaceutical/Medical Distributor Invoices (B2B GST Invoices).

TASK:
1. Extract every single line item from the main product table. Do not skip any rows.
2. Extract the summary financial totals precisely.
3. Perform or verify the mathematical calculations to ensure financial accuracy.
4. Return ONLY a valid JSON object (no markdown, no code fences, no explanation) with this exact structure:

{
  "distributor_name": "string — Supplier/Distributor name from invoice header",
  "buyer_name": "string — Buyer/Retailer pharmacy name (from 'To:', 'Sold To:', or similar; null if not visible)",
  "invoice_no": "string — Invoice/Bill number",
  "invoice_date": "string — Invoice date in DD/MM/YYYY or MM/YYYY format (null if not visible)",
  "grand_total": "number — Grand Total / Amount Payable as printed on invoice",
  "total_taxable_amount": "number — Total Taxable Amount from invoice footer (should equal sum of all line taxable_values)",
  "cgst_total": "number — Total CGST amount from invoice footer (0 if not present)",
  "sgst_total": "number — Total SGST amount from invoice footer (0 if not present)",
  "scheme_discount": "number — Total scheme/trade discount from footer (0 if not present)",
  "cash_discount": "number — Cash discount from footer (0 if not present)",
  "round_off": "number — Round off from footer (positive or negative, 0 if not present)",
  "pending_invoices_count": "number — Number of pending/unpaid invoices shown on invoice (0 if not visible)",
  "pending_total_amount": "number — Total pending/unpaid balance amount (0 if not visible)",
  "lineItems": [
    {
      "rack": "string — Rack/Shelf location code (null if not present)",
      "medicineName": "string — Product/Medicine name and strength",
      "qty_billed": "number — Quantity billed/purchased",
      "free_scm": "number — Free quantity or scheme percentage (0 if none)",
      "pack": "string — Packaging type e.g. 10'S, 15'S, 15ML, 60'S",
      "batch_no": "string — Batch/Lot number",
      "exp_date": "string — Expiry date in MM/YY or DD/MM/YYYY format",
      "mrp": "number — Maximum Retail Price per pack",
      "trade_price": "number — Trade Price per unit (before C.D.% discount)",
      "cd_percent": "number — C.D.% (cash discount percentage, usually 4.00%; 0 if absent)",
      "scm_dis_value": "number — Per-line scheme discount value in ₹ (0 if not present)",
      "taxable_value": "number — READ the printed Taxable Value from the invoice column (do NOT compute — use the actual printed number)",
      "gst_percent": "number — GST rate for THIS row (12, 18, 5, or 28 — read from each row individually)",
      "net_value": "number — READ the printed Net Value / Amount from the invoice column (do NOT compute — use the actual printed number)",
      "mfac": "string — Manufacturer code/abbreviation (e.g. CIPL, MICR, ALKE; null if not visible)",
      "hsn_code": "string — HSN code for GST classification (null if not visible)",
      "confidence": "number between 0 and 1 — how confident you are in this line's extraction accuracy"
    }
  ],
  "captureQuality": {
    "readable": "boolean — can you read the invoice clearly?",
    "issues": ["array of strings — specific problems if not readable"],
    "missingPage": "boolean — multi-page invoice with pages missing?"
  }
}

═══ COLUMN MAPPING RULES (STRICT — CRITICAL) ═══
The invoice table has fixed columns printed in this exact left-to-right order. You MUST anchor each value to its correct column header. DO NOT shift values left or right.

TYPICAL COLUMN ORDER (left to right):
[RACK] → [DESCRIPTION] → [QTY BILLED] → [FREE/SCM] → [PACK] → [BATCH] → [EXP] → [MRP] → [TRADE PRICE] → [C.D.%] → [SCM DIS] → [TAXABLE VALUE] → [GST%] → [NET VALUE]

CRITICAL ANTI-SHIFT RULES:
- If a column is EMPTY or blank for a row, put 0 (for numbers) or "" (for strings). Do NOT let subsequent column values slide into the empty slot.
- Example: If C.D.% is blank, taxable_value is NOT placed in the C.D.% slot. taxable_value stays in its own column.
- Example: If SCM DIS is blank, the taxable value stays in the TAXABLE VALUE column, NOT in the SCM DIS column.
- Each value belongs to the column directly ABOVE it in the header row. Trace vertically from the header, not horizontally from the previous cell.

FIELD-TO-COLUMN MAP:
- rack → Rack/Shelf location code (e.g. C0194, E0387)
- medicineName → Description / Product Name column
- qty_billed → Qty Billed / Quantity column
- free_scm → Free Qty / Scheme Qty / Scm% column (0 if absent)
- pack → Pack / Pack Size column (e.g. 10'S, 15ML, 60'S)
- batch_no → Batch No. / Lot No. column
- exp_date → Exp. Date / Expiry Date column
- mrp → M.R.P. column (per pack)
- trade_price → Trade Price / T.Rate / Rate column (per unit BEFORE C.D.%)
- cd_percent → C.D.% column (cash discount percentage, usually 4%)
- scm_dis_value → Scm Dis Value / Scheme Discount Value column (absolute ₹, 0 if absent)
- taxable_value → Taxable Value column (read the printed number from this column)
- gst_percent → GST % column (read from EACH ROW individually — do NOT use a flat rate)
- net_value → Net Value / Amount column (the final amount including GST)
- mfac → Mfac / Manufacturer / Company code column
- hsn_code → HSN Code column

SELF-CHECK AFTER EXTRACTION: For each row, verify that taxable_value is a reasonable number (typically between ₹10 and ₹50,000 for a single line item). If taxable_value looks like a quantity, batch number, or discount percentage, you have placed it in the wrong column — re-examine the row.

═══ FEW-SHOT EXAMPLE (how to correctly anchor columns) ═══
Below is a sample row as it appears on a printed invoice, and the correct JSON output. Note how empty columns get 0 — values do NOT slide left.

INVOICE ROW (left to right):
| Rack   | Description      | Qty | Free | Pack | Batch    | Exp    | MRP  | Trade  | C.D.% | Scm Dis | Taxable  | GST% | Net     |
| C0194  | ESSRYL M1 TAB   | 5   | 1    | 10'S | AB1234   | 03/27  | 185  | 172.80 | 4     | 0       | 829.44   | 12   | 929.97  |

CORRECT JSON for this row:
{
  "rack": "C0194",
  "medicineName": "ESSRYL M1 TAB",
  "qty_billed": 5,
  "free_scm": 1,
  "pack": "10'S",
  "batch_no": "AB1234",
  "exp_date": "03/27",
  "mrp": 185,
  "trade_price": 172.80,
  "cd_percent": 4,
  "scm_dis_value": 0,
  "taxable_value": 829.44,
  "gst_percent": 12,
  "net_value": 929.97
}

NOTICE: trade_price (172.80) is in the TRADE PRICE column. taxable_value (829.44) is in the TAXABLE VALUE column. Even though trade_price and qty_billed are close in the row, they go to DIFFERENT fields. Do NOT put 172.80 into taxable_value.

═══ ARITHMETIC FORMULA (MUST be applied to EVERY row) ═══
1. taxable_value = trade_price × qty_billed × (1 - cd_percent/100)
2. gst_amount = taxable_value × gst_percent / 100
3. net_value = taxable_value + gst_amount
Use the printed values from the invoice columns as the primary source. Only apply these formulas as a fallback when a printed value is missing or unreadable. If a printed value exists but differs from the formula result, use the printed value — distributors may apply scheme adjustments or tiered discounts that change the standard formula.

═══ GST BREAKDOWN (Summary Level) ═══
CGST and SGST are split equally. If GST is 12%, CGST = 6% and SGST = 6%.
Group taxable amounts by their GST slabs and compute exact CGST/SGST totals.
- 12% GST slab → 6% CGST + 6% SGST
- 18% GST slab → 9% CGST + 9% SGST

═══ CROSS-VERIFICATION (MUST DO BEFORE OUTPUTTING) ═══
Before you output the final JSON, perform these checks internally and CORRECT any mismatches:
1. Sum all line taxable_values → this MUST match total_taxable_amount from the footer. If it doesn't, re-examine each line — you likely shifted a column.
2. Sum all line net_values → this should be close to grand_total (within ₹10 for rounding).
3. grand_total ≈ total_taxable_amount + cgst_total + sgst_total - scheme_discount - cash_discount + round_off
4. If any line's taxable_value is less than ₹5 and trade_price > ₹20, you almost certainly put the trade price or quantity in the wrong column. Fix it before outputting.
5. Report any remaining discrepancies as a note, but always use the printed value from the invoice column.

═══ GENERAL RULES ═══
- Extract EVERY line item visible, even if partially visible
- For confidence: 1.0 = perfect, 0.9 = very clear, 0.8 = minor doubt, 0.7 = partially readable, <0.7 = uncertain
- If a field is not visible, use null for strings and 0 for numbers
- Read gst_percent from EACH ROW — do NOT apply a single flat GST rate to all lines
- mfac should be the manufacturer abbreviation/code as printed (CIPL, MICR, ALKE, etc.)
- pack should include the unit (10'S, 15'S, 15ML, 60'S, 3'S, 120'S, etc.)
- For captureQuality.readable: false if >30% of invoice is unreadable"""


# ─── PDF Splitting ───────────────────────────────────────────────

def split_pdf(pdf_path, dpi=200, output_dir=None):
    """Split PDF into JPEG page images. Returns list of file paths."""
    if output_dir is None:
        base = os.path.splitext(os.path.basename(pdf_path))[0]
        output_dir = os.path.join(os.path.dirname(os.path.abspath(pdf_path)), f"{base}_pages")

    os.makedirs(output_dir, exist_ok=True)
    doc = fitz.open(pdf_path)
    total = len(doc)
    zoom = dpi / 72
    matrix = fitz.Matrix(zoom, zoom)
    paths = []

    print(f"\n  Splitting {total} pages at {dpi} DPI...")

    for i in range(total):
        page = doc[i]
        pix = page.get_pixmap(matrix=matrix)
        jpg_path = os.path.join(output_dir, f"page_{i + 1:03d}.jpg")
        pix.pil_save(jpg_path, format="JPEG", quality=92)
        paths.append(jpg_path)

        if (i + 1) % 10 == 0 or i == total - 1:
            size_kb = os.path.getsize(jpg_path) / 1024
            print(f"  [{i + 1:>3}/{total}] {os.path.basename(jpg_path)} ({size_kb:.0f} KB)")

    doc.close()
    return paths, output_dir


# ─── Gemini API ──────────────────────────────────────────────────

def call_gemini(image_path, api_key):
    """Send a page image to Gemini and return parsed JSON."""
    with open(image_path, "rb") as f:
        img_data = base64.b64encode(f.read()).decode("utf-8")

    payload = {
        "contents": [{"parts": [
            {"text": EXTRACTION_PROMPT},
            {"inlineData": {"mimeType": "image/jpeg", "data": img_data}}
        ]}],
        "generationConfig": {
            "temperature": 0.1,
            "topK": 1,
            "topP": 1,
            "maxOutputTokens": 8192,
            "responseMimeType": "application/json"
        },
        "safetySettings": [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
        ]
    }

    resp = requests.post(
        f"{GEMINI_API_URL}?key={api_key}",
        json=payload,
        timeout=120
    )
    resp.raise_for_status()

    text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
    return json.loads(text)


# ─── Worker ──────────────────────────────────────────────────────

def extract_page(args):
    """Extract a single page. Returns (page_num, result_or_None)."""
    page_path, page_num, total, api_key = args
    try:
        result = call_gemini(page_path, api_key)
        items = len(result.get("lineItems", []))
        dist = result.get("distributor_name", "Unknown")
        return (page_num, result, None)
    except Exception as e:
        return (page_num, None, str(e))


# ─── Main ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Split a large PDF and extract invoices via Gemini (no browser needed)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python process_large_pdf.py invoices_300mb.pdf
  python process_large_pdf.py big.pdf --dpi 250 --concurrency 5
  python process_large_pdf.py big.pdf --save-pages --json-only
        """
    )
    parser.add_argument("pdf", help="Path to the PDF file")
    parser.add_argument("--dpi", type=int, default=200, help="Page render resolution (default: 200)")
    parser.add_argument("--concurrency", type=int, default=3, help="Parallel Gemini calls (default: 3)")
    parser.add_argument("--output", default=None, help="Output dir for page images")
    parser.add_argument("--save-pages", action="store_true", help="Keep page images after extraction")
    parser.add_argument("--json-only", action="store_true", help="Split only — don't call Gemini")

    args = parser.parse_args()

    if not os.path.exists(args.pdf):
        print(f"\n  ERROR: File not found: {args.pdf}")
        sys.exit(1)

    # Get API key
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key and not args.json_only:
        # Try loading from .env file
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("GEMINI_API_KEY="):
                        api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break

    if not api_key and not args.json_only:
        print("\n  ERROR: GEMINI_API_KEY not set.")
        print("  Set it via:")
        print("    set GEMINI_API_KEY=your_key_here       (Windows CMD)")
        print("    $env:GEMINI_API_KEY='your_key_here'     (PowerShell)")
        print("    export GEMINI_API_KEY=your_key_here     (Mac/Linux)")
        print("  Or create a .env file with: GEMINI_API_KEY=your_key_here\n")
        sys.exit(1)

    file_size_mb = os.path.getsize(args.pdf) / (1024 * 1024)

    print(f"\n{'=' * 60}")
    print(f"  RxExpiry — Large PDF Processor")
    print(f"  PDF:    {os.path.basename(args.pdf)} ({file_size_mb:.1f} MB)")
    print(f"  DPI:    {args.dpi}")
    print(f"  Workers: {args.concurrency}")
    print(f"  Model:  {GEMINI_MODEL}")
    print(f"{'=' * 60}")

    # Step 1: Split PDF
    page_paths, output_dir = split_pdf(args.pdf, dpi=args.dpi, output_dir=args.output)

    if args.json_only:
        print(f"\n  Done — {len(page_paths)} page images saved to: {output_dir}/")
        return

    # Step 2: Extract each page via Gemini (parallel)
    print(f"\n  Extracting invoices from {len(page_paths)} pages ({args.concurrency} workers)...\n")

    results = [None] * len(page_paths)
    errors = []
    completed = 0
    total = len(page_paths)
    start_time = time.time()

    work_items = [(path, i + 1, total, api_key) for i, path in enumerate(page_paths)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        future_map = {pool.submit(extract_page, item): item[1] for item in work_items}

        for future in concurrent.futures.as_completed(future_map):
            page_num = future_map[future]
            pn, result, err = future.result()
            completed += 1

            if result:
                results[pn - 1] = result
                items = len(result.get("lineItems", []))
                dist = result.get("distributor_name", "?")
                print(f"  [{completed:>3}/{total}] Page {pn}: {items} items — {dist}")
            else:
                errors.append((pn, err))
                print(f"  [{completed:>3}/{total}] Page {pn}: FAILED — {err[:60]}")

            elapsed = time.time() - start_time
            if completed > 1:
                rate = completed / elapsed
                eta = (total - completed) / rate
                print(f"           Rate: {rate:.1f}/s | ETA: {eta:.0f}s")

    # Step 3: Save results
    elapsed = time.time() - start_time
    successful = sum(1 for r in results if r and r.get("lineItems"))
    total_items = sum(len(r.get("lineItems", [])) for r in results if r)

    output_json = os.path.splitext(args.pdf)[0] + "_extraction.json"
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump({
            "source_pdf": os.path.basename(args.pdf),
            "pdf_size_mb": round(file_size_mb, 1),
            "pages_total": total,
            "pages_extracted": successful,
            "pages_failed": len(errors),
            "total_line_items": total_items,
            "extraction_time_seconds": round(elapsed, 1),
            "errors": [{"page": p, "error": e} for p, e in errors],
            "results": results,
        }, f, indent=2, ensure_ascii=False)

    print(f"\n{'=' * 60}")
    print(f"  EXTRACTION COMPLETE")
    print(f"  Pages: {successful}/{total} extracted | Items: {total_items}")
    print(f"  Errors: {len(errors)}")
    print(f"  Time: {elapsed:.1f}s ({total / elapsed:.1f} pages/sec)")
    print(f"  Results: {output_json}")
    print(f"{'=' * 60}")

    # Step 4: Cleanup
    if not args.save_pages:
        import shutil
        shutil.rmtree(output_dir, ignore_errors=True)
        print(f"\n  Cleaned up page images from {output_dir}/")
    else:
        print(f"\n  Page images kept at: {output_dir}/")


if __name__ == "__main__":
    main()
