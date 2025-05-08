import os
import json
import re
from pathlib import Path

# Input/output directories
input_dir = "/Users/sarasaad/Documents/BDMA /UPC/BDM/Project/BDM Scholamigo/ScholAmigo_BDM/trusted_zone/data/resume_data"
output_dir = "/Users/sarasaad/Documents/BDMA /UPC/BDM/Project/BDM Scholamigo/ScholAmigo_BDM/trusted_zone/data/resume_data/cleaned/"
Path(output_dir).mkdir(parents=True, exist_ok=True)

def normalize_text(text):
    text = text.lower()
    text = re.sub(r"\s+", " ", text)  # collapse all whitespace
    return text.strip()

def clean_resume_sections(data):
    if not isinstance(data, dict) or "file_name" not in data or "sections" not in data:
        return None  # invalid format

    cleaned = {
        "file_name": data["file_name"],
        "sections": {
            k: normalize_text(v) for k, v in data["sections"].items() if isinstance(v, str)
        }
    }
    return cleaned

def process_all():
    for filename in os.listdir(input_dir):
        if filename.endswith(".json"):
            try:
                with open(os.path.join(input_dir, filename), "r", encoding="utf-8") as f:
                    data = json.load(f)

                cleaned = clean_resume_sections(data)
                if cleaned:
                    output_path = os.path.join(output_dir, filename)
                    with open(output_path, "w", encoding="utf-8") as out:
                        json.dump(cleaned, out, ensure_ascii=False, indent=2)
                    print(f"✅ Processed: {filename}")
                else:
                    print(f"⚠️  Skipped (invalid format): {filename}")

            except Exception as e:
                print(f"Error in {filename}: {e}")

if __name__ == "__main__":
    process_all()
