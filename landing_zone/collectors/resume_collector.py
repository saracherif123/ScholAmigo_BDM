#!/usr/bin/env python
# coding: utf-8

# In[16]:


import kagglehub
import os
import shutil

# Download Kaggle dataset (stored in cache by default)
source_path = kagglehub.dataset_download("snehaanbhawal/resume-dataset")
print("✅ Downloaded to:", source_path)

# Define the target directory relative to the current working directory
# This avoids using __file__ and works in notebooks
BASE_DIR = os.getcwd()
target_dir = os.path.join(BASE_DIR, "..", "data", "resume_data")
os.makedirs(target_dir, exist_ok=True)

# Recursively copy all files from the Kaggle download directory
files_copied = 0
for root, _, files in os.walk(source_path):
    for file in files:
        src = os.path.join(root, file)
        dst = os.path.join(target_dir, file)
        shutil.copy2(src, dst)
        print(f"📁 Copied: {file}")
        files_copied += 1

print(f"\n✅ Done. {files_copied} files copied to: {target_dir}")


# In[ ]:




