import os
import pandas as pd
import json

# Paths
DATA_DIR = "landing_zone/data"
SCHEMA_DIR = "metadata/schemas"
EXCLUDED_DIRS = {"erasmus_data"}

os.makedirs(SCHEMA_DIR, exist_ok=True)

def infer_schema_from_df(df):
    schema = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        if "int" in dtype:
            col_type = "integer"
        elif "float" in dtype:
            col_type = "number"
        elif "bool" in dtype:
            col_type = "boolean"
        elif "datetime" in dtype:
            col_type = "datetime"
        else:
            col_type = "string"
        schema[col] = col_type
    return schema

# Loop through files
for subdir, _, files in os.walk(DATA_DIR):
    if any(excluded in subdir for excluded in EXCLUDED_DIRS):
        continue

    for file in files:
        if file.endswith((".csv", ".json", ".jsonl")):
            file_path = os.path.join(subdir, file)
            relative_path = os.path.relpath(file_path, DATA_DIR)

            try:
                # Special case: resume_data is HTML in JSON
                if "resume_data" in subdir:
                    schema = {"html": "string"}
                elif file.endswith(".csv"):
                    df = pd.read_csv(file_path)
                    schema = infer_schema_from_df(df)
                elif file.endswith(".json"):
                    df = pd.read_json(file_path)
                    schema = infer_schema_from_df(df)
                elif file.endswith(".jsonl"):
                    df = pd.read_json(file_path, lines=True)
                    schema = infer_schema_from_df(df)
                else:
                    continue

                # Save schema
                schema_filename = os.path.splitext(file)[0] + "_schema.json"
                schema_path = os.path.join(SCHEMA_DIR, schema_filename)
                with open(schema_path, "w") as f:
                    json.dump(schema, f, indent=2)

                print(f" Schema generated for: {relative_path}")

            except Exception as e:
                print(f" Skipped {relative_path} — {e}")
