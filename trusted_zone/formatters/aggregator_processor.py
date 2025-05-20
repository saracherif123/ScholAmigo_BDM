from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, regexp_replace, lower, trim, explode
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
import re
from bs4 import BeautifulSoup
import country_converter as coco
from datetime import datetime
import json

def create_spark_session():
    return SparkSession.builder \
        .appName("AggregatorProcessor") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.665") \
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
            
            return f"{day.zfill(2)}/{month.zfill(2)}/{year}"
    
    return ""

def process_scholarship(scholarship):
    # Initialize default values
    info = {
        "scholarship_name": "",
        "description": "",
        "countries": [],
        "scholarship_type": "",
        "scholarship_amount": {
            "euro": "",
            "original": ""
        },
        "requirements": "",
        "required_documents": "",
        "level": "",
        "field_of_study": "",
        "universities": [],
        "deadline": "",
        "admission": "",
        "website": "",
        "course_content": "",
        "duration": "",
        "status": "active",
        "other": []
    }
    
    # Clean and process text fields
    if "scholarship_name" in scholarship:
        info["scholarship_name"] = clean_text(scholarship["scholarship_name"])
    
    if "description" in scholarship:
        info["description"] = clean_text(scholarship["description"])
    
    # Process countries
    if "countries" in scholarship:
        cc = coco.CountryConverter()
        countries = scholarship["countries"]
        if isinstance(countries, str):
            countries = [countries]
        iso_codes = cc.convert(countries, to='ISO3')
        info["countries"] = [code for code in iso_codes if code != 'not found']
    
    # Process scholarship type
    if "scholarship_type" in scholarship:
        stype = scholarship["scholarship_type"].lower()
        if "full" in stype or "fully" in stype:
            info["scholarship_type"] = "fully_funded"
        elif "partial" in stype:
            info["scholarship_type"] = "partial"
    
    # Process amount
    if "scholarship_amount" in scholarship:
        amount = scholarship["scholarship_amount"]
        if isinstance(amount, dict):
            if "original" in amount:
                info["scholarship_amount"]["original"] = amount["original"]
            if "euro" in amount:
                info["scholarship_amount"]["euro"] = amount["euro"]
        else:
            info["scholarship_amount"]["original"] = str(amount)
    
    # Process deadline
    if "deadline" in scholarship:
        info["deadline"] = standardize_date(scholarship["deadline"])
    
    # Process level
    if "level" in scholarship:
        level = scholarship["level"].lower()
        if "bachelor" in level or "undergraduate" in level:
            info["level"] = "Bachelor"
        elif "master" in level or "postgraduate" in level:
            info["level"] = "Master"
        elif "phd" in level or "doctoral" in level:
            info["level"] = "PhD"
    
    # Process admission period
    if "admission" in scholarship:
        admission = scholarship["admission"].lower()
        if "fall" in admission or "autumn" in admission or "september" in admission:
            info["admission"] = "fall"
        elif "spring" in admission or "january" in admission:
            info["admission"] = "spring"
    
    # Process duration
    if "duration" in scholarship:
        duration = scholarship["duration"]
        if isinstance(duration, str):
            match = re.search(r'(\d+)\s*(?:year|yr)s?', duration.lower())
            if match:
                info["duration"] = f"{match.group(1)} years"
    
    # Check status
    if "status" in scholarship:
        status = scholarship["status"].lower()
        if "closed" in status or "ended" in status:
            info["status"] = "closed"
    
    return info

def process_aggregator_data():
    spark = create_spark_session()
    
    # Read JSON files from S3
    df = spark.read.json("s3a://scholamigo/landing_zone_data/aggregator_data/")
    
    # Process scholarships
    process_scholarship_udf = udf(process_scholarship, 
                                 StructType([
                                     StructField("scholarship_name", StringType(), True),
                                     StructField("description", StringType(), True),
                                     StructField("countries", ArrayType(StringType()), True),
                                     StructField("scholarship_type", StringType(), True),
                                     StructField("scholarship_amount", 
                                               StructType([
                                                   StructField("euro", StringType(), True),
                                                   StructField("original", StringType(), True)
                                               ]), True),
                                     StructField("requirements", StringType(), True),
                                     StructField("required_documents", StringType(), True),
                                     StructField("level", StringType(), True),
                                     StructField("field_of_study", StringType(), True),
                                     StructField("universities", ArrayType(StringType()), True),
                                     StructField("deadline", StringType(), True),
                                     StructField("admission", StringType(), True),
                                     StructField("website", StringType(), True),
                                     StructField("course_content", StringType(), True),
                                     StructField("duration", StringType(), True),
                                     StructField("status", StringType(), True),
                                     StructField("other", ArrayType(StringType()), True)
                                 ]))
    
    # Process the data
    processed_df = df.select(
        process_scholarship_udf(col("*")).alias("scholarship_info")
    )
    
    # Filter out closed scholarships
    processed_df = processed_df.filter(col("scholarship_info.status") == "active")
    
    # Remove duplicates based on scholarship name
    processed_df = processed_df.dropDuplicates(["scholarship_info.scholarship_name"])
    
    # Write to S3
    processed_df.write.mode("overwrite").json("s3a://scholamigo/trusted_zone_data/aggregator_data/processed/")
    
    spark.stop()

if __name__ == "__main__":
    process_aggregator_data() 