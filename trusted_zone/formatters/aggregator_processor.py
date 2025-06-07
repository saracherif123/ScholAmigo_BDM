from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, regexp_replace, lower, trim, explode, struct
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
import re
from bs4 import BeautifulSoup
import country_converter as coco
from datetime import datetime
import json
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("AggregatorProcessor") \
        .getOrCreate()

def clean_text(text):
    if not text:
        return ""
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s.,;:!?()-]', ' ', text)
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def standardize_date(date_str):
    if not date_str:
        return ""
    
    # Try different date formats
    formats = [
        r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',  # DD/MM/YYYY or DD-MM-YYYY
        r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # YYYY/MM/DD or YYYY-MM-DD
        r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})'   # DD Month YYYY
    ]
    
    for fmt in formats:
        match = re.search(fmt, date_str)
        if match:
            if fmt == formats[0]:  # DD/MM/YYYY
                day, month, year = match.groups()
            elif fmt == formats[1]:  # YYYY/MM/DD
                year, month, day = match.groups()
            else:  # DD Month YYYY
                day, month, year = match.groups()
                # Convert month name to number
                try:
                    month = datetime.strptime(month, '%B').month
                except:
                    try:
                        month = datetime.strptime(month, '%b').month
                    except:
                        return ""
            
            # Convert to integers first
            try:
                day = int(day)
                month = int(month)
                year = int(year)
                # Then convert to strings and pad with zeros
                return f"{str(day).zfill(2)}/{str(month).zfill(2)}/{year}"
            except ValueError:
                return ""
    
    return ""

def process_scholarship(scholarship):
    # Extract and clean countries
    countries_str = scholarship["You must be studying in one of the following countries:"] if "You must be studying in one of the following countries:" in scholarship else ""
    countries = []
    if countries_str:
        # Split by " and " and clean each country
        countries = [c.strip() for c in countries_str.split(" and ") if c.strip()]
        # If no countries found, check if it's "Unrestricted"
        if not countries and "unrestricted" in countries_str.lower():
            countries = ["Unrestricted"]

    # Extract and clean amount
    amount_str = scholarship["Amount"] if "Amount" in scholarship else ""
    amount = ""
    if amount_str:
        # Remove currency symbols and commas
        amount = re.sub(r'[$,]', '', amount_str).strip()
        # Convert to numeric format
        try:
            amount = f"${float(amount):,.2f}"
        except ValueError:
            amount = amount_str

    # Extract and clean field of study
    field_str = scholarship["You must be studying one of the following:"] if "You must be studying one of the following:" in scholarship else ""
    fields = []
    if field_str and field_str.lower() != "unrestricted":
        # Split by commas and clean each field
        fields = [f.strip() for f in field_str.split(",") if f.strip()]

    # Extract and standardize deadline
    deadline_str = scholarship["Deadline"] if "Deadline" in scholarship else ""
    deadline = standardize_date(deadline_str) if deadline_str else ""

    # Extract level from description and requirements
    description = scholarship["Description"] if "Description" in scholarship else ""
    requirements = scholarship["Other Criteria"] if "Other Criteria" in scholarship else ""
    
    # Convert to lowercase safely
    description_lower = description.lower() if description else ""
    requirements_lower = requirements.lower() if requirements else ""
    
    level = ""
    if any(word in description_lower or word in requirements_lower 
           for word in ["bachelor", "undergraduate"]):
        level = "Bachelor"
    elif any(word in description_lower or word in requirements_lower 
             for word in ["master", "postgraduate"]):
        level = "Master"
    elif any(word in description_lower or word in requirements_lower 
             for word in ["phd", "doctoral"]):
        level = "PhD"

    # Extract scholarship type from description and requirements
    scholarship_type = ""
    if any(word in description_lower or word in requirements_lower 
           for word in ["full", "fully", "complete"]):
        scholarship_type = "fully_funded"
    elif any(word in description_lower or word in requirements_lower 
             for word in ["partial", "part"]):
        scholarship_type = "partial"

    info = {
        "scholarship_name": clean_text(scholarship["Title"] if "Title" in scholarship else ""),
        "description": clean_text(scholarship["Description"] if "Description" in scholarship else ""),
        "countries": countries,
        "scholarship_type": scholarship_type,
        "scholarship_amount": {
            "euro": "",
            "original": amount
        },
        "requirements": clean_text(scholarship["Other Criteria"] if "Other Criteria" in scholarship else ""),
        "required_documents": "",
        "level": level,
        "field_of_study": fields,
        "universities": [],
        "deadline": deadline,
        "admission": "",
        "website": "",
        "course_content": "",
        "duration": "",
        "status": "active",
        "other": []
    }
    
    return info

def process_aggregator_data():
    spark = create_spark_session()
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    input_path = os.path.join(project_root, "landing_zone", "data", "aggregator_data", "scholarships_aggregate.json")
    output_path = os.path.join(project_root, "trusted_zone", "data", "aggregator_data", "processed")
    os.makedirs(output_path, exist_ok=True)

    print(f"Reading data from: {input_path}")
    # Read JSON file line by line (NDJSON format)
    with open(input_path, 'r') as f:
        json_objects = json.load(f)
    # Convert to DataFrame
    df = spark.createDataFrame(json_objects)
    print("Sample of loaded data:")
    df.show(5, truncate=False)

    # Handle corrupt records
    if '_corrupt_record' in df.columns:
        corrupt_count = df.filter(col('_corrupt_record').isNotNull()).count()
        print(f"Corrupt records found: {corrupt_count}")
        df = df.filter(col('_corrupt_record').isNull())
        if df.count() == 0:
            print("No valid records to process. Exiting.")
            return

    # Define UDF and schema
    process_scholarship_udf = udf(process_scholarship, StructType([
        StructField("scholarship_name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("countries", ArrayType(StringType()), True),
        StructField("scholarship_type", StringType(), True),
        StructField("scholarship_amount", StructType([
            StructField("euro", StringType(), True),
            StructField("original", StringType(), True)
        ]), True),
        StructField("requirements", StringType(), True),
        StructField("required_documents", StringType(), True),
        StructField("level", StringType(), True),
        StructField("field_of_study", ArrayType(StringType()), True),
        StructField("universities", ArrayType(StringType()), True),
        StructField("deadline", StringType(), True),
        StructField("admission", StringType(), True),
        StructField("website", StringType(), True),
        StructField("course_content", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("status", StringType(), True),
        StructField("other", ArrayType(StringType()), True)
    ]))

    print("Processing scholarships...")
    processed_df = df.select(process_scholarship_udf(struct([col(c) for c in df.columns])).alias("scholarship_info"))
    processed_df = processed_df.withColumn("scholarship_name", col("scholarship_info.scholarship_name"))
    processed_df = processed_df.dropDuplicates(["scholarship_name"])

    print(f"Writing processed data to: {output_path}")
    processed_df.write.mode("overwrite").json(output_path)
    print("Processing complete!")
    spark.stop()

if __name__ == "__main__":
    process_aggregator_data() 