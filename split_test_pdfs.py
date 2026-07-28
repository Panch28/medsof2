import os
import glob
from SU1.pdf_splitter import split_pdf_to_images

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_folder = os.path.join(base_dir, "test_output")
    
    folders_to_check = ["test", "test_invoices"]
    
    pdf_files = []
    for folder in folders_to_check:
        folder_path = os.path.join(base_dir, folder)
        if os.path.exists(folder_path):
            pdfs = glob.glob(os.path.join(folder_path, "*.pdf"))
            pdf_files.extend(pdfs)
            
    if not pdf_files:
        print("No PDF files found in 'test' or 'test_invoices' directories.")
        return
        
    for input_pdf_path in pdf_files:
        target_pdf_name = os.path.basename(input_pdf_path)
        pdf_basename = os.path.splitext(target_pdf_name)[0]
        specific_output_folder = os.path.join(output_folder, pdf_basename)
        
        safe_name = target_pdf_name.encode('ascii', 'ignore').decode('ascii')
        print(f"\n----------------------------------------")
        print(f"Processing '{safe_name}'...")
        
        split_pdf_to_images(input_pdf_path, specific_output_folder)
        
        safe_folder = specific_output_folder.encode('ascii', 'ignore').decode('ascii')
        print(f"-> Successfully saved images to: {safe_folder}")

if __name__ == "__main__":
    main()
