import os
import glob

base_dir = "data/rns/"

folders = [f for f in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, f))]
if not folders:
    print("No folders found in", base_dir)
    exit(1)

latest_folder = sorted(folders)[-1]
latest_path = os.path.join(base_dir, latest_folder)
print(f"Processing PDFs in latest folder: {latest_path}")

pdfs = glob.glob(os.path.join(latest_path, "*.pdf"))
if not pdfs:
    print(f"No PDFs found in {latest_path}")
    exit(0)
