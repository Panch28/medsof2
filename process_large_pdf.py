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
import re
import time
import base64
import argparse
import concurrent.futures
import fitz  # PyMuPDF
import requests

# ─── Gemini Config ───────────────────────────────────────────────
GEMINI_MODEL = "gemini-3.5-flash-lite"
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

EXTRACTION_PROMPT = r"""Role & Objective:
You are an advanced, high-precision OCR and invoice parsing engine specialized in Indian Pharmaceutical GST Invoices. Your task is to extract all line items, metadata, tax distributions, and financial totals from the provided invoice image/text with 100% mathematical accuracy and zero omissions. 

CRITICAL INSTRUCTIONS FOR LOCAL SOFTWARE INTEGRATION:
1. EXHAUSTIVE EXTRACTION: Do not truncate or summarize tables. Extract EVERY single row present in the item table. If an item spans multiple lines, merge them correctly.
2. STRICT MATHEMATICAL VALIDATION: 
   - Check that Taxable Value + GST = Net Value for every row.
   - Sum all Net Values and apply discounts, total GST, and round-offs mathematically before outputting the Grand Total. Do not guess numbers; calculate them based on the text.
3. HANDLING MISSING DATA: If a specific column value (like RACK or HSN) is missing or blank, explicitly output null or "—" rather than shifting table cells out of alignment.

OUTPUT FORMAT REQUIREMENTS:
Structure your response into clean, programmatic sections that can be easily parsed by software (JSON-friendly markdown layout):

### 1. INVOICE METADATA
- Supplier Name: [Value]
- Supplier Address: [Value]
- Supplier GSTIN: [Value]
- Supplier State Code: [Value]
- Supplier Contact Info: [Value]
- Customer/Buyer Name: [Value]
- Customer/Buyer Address: [Value]
- Customer/Buyer GSTIN: [Value]
- Customer/Buyer PAN: [Value]
- Relationship No: [Value]
- Customer/Buyer Phone: [Value]
- Invoice No: [Value]
- Invoice Date: [Value]
- Time: [Value]
- Due Date: [Value]
- Order No: [Value]
- Sales Executive: [Value]

### 2. ITEMIZED TABLE (Strict Column Mapping)
Extract every row into a structured table containing these exact headers:
| RACK | DESCRIPTION | Billed Qty | Free Qty | Pack | Batch No. | Exp. Date | MRP (₹) | Trade Price (₹) | CD % | Taxable Value (₹) | GST % | Net Value (₹) | Mfr/Mkt | HSN Code |

### 3. TAX BREAKDOWN & FINANCIAL SUMMARY
- Total Items: [Value]
- Total Billed Quantity: [Value]
- Total Taxable Sale Amount: [Value]
- Slab-wise Tax breakdown: [Value]
- Scheme Discount: [Value]
- Cash Discount: [Value]
- Total GST: [Value]
- TCS: [Value]
- Credit Notes: [Value]
- Round Off: [Value]
- GRAND TOTAL PAYABLE: [Value]

### 4. OUTSTANDING / LEDGER DETAILS
- Pending Invoices: [Value]
- Total Outstanding Balance: [Value]

Execute this extraction with absolute fidelity to the source document. No approximations."""


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

def parse_markdown_invoice_py(text):
    result = {
        "distributor_name": "",
        "buyer_name": None,
        "invoice_no": "",
        "invoice_date": None,
        "grand_total": 0.0,
        "total_taxable_amount": 0.0,
        "cgst_total": 0.0,
        "sgst_total": 0.0,
        "scheme_discount": 0.0,
        "cash_discount": 0.0,
        "round_off": 0.0,
        "pending_invoices_count": 0,
        "pending_total_amount": 0.0,
        "lineItems": [],
        "captureQuality": {
            "readable": True,
            "issues": [],
            "missingPage": False
        }
    }
    
    lines = text.split('\n')
    in_table = False
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        match = re.match(r'^[-*]\s*([^:]+)\s*:\s*(.*)$', line)
        if match:
            key = match.group(1).strip().lower()
            val = match.group(2).strip()
            clean_val = "" if val in ['null', '—', 'undefined'] else val
            
            if 'supplier name' in key:
                result["distributor_name"] = clean_val
            elif 'customer/buyer name' in key or 'buyer name' in key:
                result["buyer_name"] = clean_val if clean_val else None
            elif 'invoice no' in key:
                result["invoice_no"] = clean_val
            elif 'invoice date' in key:
                result["invoice_date"] = clean_val if clean_val else None
            elif 'total taxable sale amount' in key:
                try:
                    result["total_taxable_amount"] = float(re.sub(r'[^\d.-]', '', clean_val))
                except ValueError:
                    result["total_taxable_amount"] = 0.0
            elif 'scheme discount' in key:
                try:
                    result["scheme_discount"] = float(re.sub(r'[^\d.-]', '', clean_val))
                except ValueError:
                    result["scheme_discount"] = 0.0
            elif 'cash discount' in key:
                try:
                    result["cash_discount"] = float(re.sub(r'[^\d.-]', '', clean_val))
                except ValueError:
                    result["cash_discount"] = 0.0
            elif 'total gst' in key:
                try:
                    total_gst = float(re.sub(r'[^\d.-]', '', clean_val))
                    result["cgst_total"] = round(total_gst / 2.0, 2)
                    result["sgst_total"] = round(total_gst / 2.0, 2)
                except ValueError:
                    pass
            elif 'round off' in key:
                try:
                    result["round_off"] = float(re.sub(r'[^\d.-]', '', clean_val))
                except ValueError:
                    result["round_off"] = 0.0
            elif 'grand total payable' in key or 'grand total' in key:
                try:
                    result["grand_total"] = float(re.sub(r'[^\d.-]', '', clean_val))
                except ValueError:
                    result["grand_total"] = 0.0
            elif 'pending invoices' in key:
                try:
                    result["pending_invoices_count"] = int(re.sub(r'[^\d]', '', clean_val))
                except ValueError:
                    result["pending_invoices_count"] = 0
            elif 'total outstanding balance' in key:
                try:
                    result["pending_total_amount"] = float(re.sub(r'[^\d.-]', '', clean_val))
                except ValueError:
                    result["pending_total_amount"] = 0.0
            continue
            
        if line.startswith('|'):
            if 'description' in line.lower() or '---' in line:
                in_table = True
                continue
            if in_table:
                parts = [p.strip() for p in line.split('|')]
                if parts and parts[0] == '':
                    parts.pop(0)
                if parts and parts[-1] == '':
                    parts.pop()
                
                if len(parts) >= 13:
                    rack = "" if parts[0] in ['—', 'null', '-'] else parts[0]
                    description = parts[1]
                    try:
                        qty_billed = int(parts[2])
                    except ValueError:
                        qty_billed = 0
                    try:
                        free_scm = float(parts[3])
                    except ValueError:
                        free_scm = 0.0
                    pack = "" if parts[4] in ['—', 'null', '-'] else parts[4]
                    batch_no = parts[5]
                    exp_date = parts[6]
                    try:
                        mrp = float(re.sub(r'[^\d.-]', '', parts[7]))
                    except ValueError:
                        mrp = 0.0
                    try:
                        trade_price = float(re.sub(r'[^\d.-]', '', parts[8]))
                    except ValueError:
                        trade_price = 0.0
                    try:
                        cd_percent = float(re.sub(r'[^\d.-]', '', parts[9]))
                    except ValueError:
                        cd_percent = 0.0
                    try:
                        taxable_value = float(re.sub(r'[^\d.-]', '', parts[10]))
                    except ValueError:
                        taxable_value = 0.0
                    try:
                        gst_percent = float(re.sub(r'[^\d.-]', '', parts[11]))
                    except ValueError:
                        gst_percent = 0.0
                    try:
                        net_value = float(re.sub(r'[^\d.-]', '', parts[12]))
                    except ValueError:
                        net_value = 0.0
                    mfac = "" if len(parts) <= 13 or parts[13] in ['—', 'null', '-'] else parts[13]
                    hsn_code = "" if len(parts) <= 14 or parts[14] in ['—', 'null', '-'] else parts[14]
                    
                    result["lineItems"].append({
                        "rack": rack if rack else None,
                        "medicineName": description,
                        "qty_billed": qty_billed,
                        "free_scm": free_scm,
                        "pack": pack,
                        "batch_no": batch_no,
                        "exp_date": exp_date,
                        "mrp": mrp,
                        "trade_price": trade_price,
                        "cd_percent": cd_percent,
                        "scm_dis_value": 0.0,
                        "taxable_value": taxable_value,
                        "gst_percent": gst_percent,
                        "net_value": net_value,
                        "mfac": mfac if mfac else None,
                        "hsn_code": hsn_code if hsn_code else None,
                        "confidence": 0.95
                    })
    return result


def call_gemini(image_path, api_key):
    """Send a page image to Gemini and return parsed data."""
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
            "maxOutputTokens": 8192
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
    return parse_markdown_invoice_py(text)


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
