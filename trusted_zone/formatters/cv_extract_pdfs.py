import os
import re
import json
from pathlib import Path
import pdfplumber

# Input/output directories
input_dir = "SCHOLAMIGO_BDM/landing_zone/data"
output_dir = "SCHOLAMIGO_BDM/trusted_zone/data/resume_data"
Path(output_dir).mkdir(parents=True, exist_ok=True)

# Define common section headers (case-insensitive)
SECTION_HEADERS = [
    "summary", "objective", "education", "experience", "work experience",
    "professional experience", "skills", "projects", "certifications",
    "courses", "languages", "awards", "publications", "interests"
]

def extract_text(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)

def split_into_sections(text):
    sections = {}
    current_section = "general"
    buffer = []

    # Build regex pattern for section headers
    pattern = re.compile(rf"^({'|'.join(SECTION_HEADERS)})(\s*[:\-]?\s*)$", re.I | re.M)

    lines = text.splitlines()
    for line in lines:
        clean_line = line.strip()
        if not clean_line:
            continue

        match = pattern.match(clean_line.lower())
        if match:
            if buffer:
                sections[current_section] = "\n".join(buffer).strip()
                buffer = []
            current_section = match.group(1).lower()
        else:
            buffer.append(clean_line)

    if buffer:
        sections[current_section] = "\n".join(buffer).strip()

    return sections

def process_all_pdfs():
    for filename in os.listdir(input_dir):
        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(input_dir, filename)
            full_text = extract_text(pdf_path)
            sections = split_into_sections(full_text)

            result = {
                "file_name": filename,
                "sections": sections
            }

            output_path = os.path.join(output_dir, filename.replace(".pdf", ".json"))
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

            print(f"✅ Processed: {filename}")

if __name__ == "__main__":
    process_all_pdfs()
