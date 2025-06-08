from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import country_converter as coco
from datetime import datetime
import re
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, BooleanType, IntegerType, MapType
from datetime import datetime
from pyspark.sql.functions import col, coalesce
import os
from pyspark.sql import SparkSession

PATH = "trusted_zone/data/uk_data"

local_parquet_path = "trusted_zone/data/uk_data/clean_uk.parquet"

aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')


spark = SparkSession.builder \
    .appName("ReadukFromS3") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.665") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.maxResultSize", "1g") \
    .getOrCreate()

# UDF to convert all dates to DD/MM/YYYY
def format_dates_to_ddmmyyyy(dates):
    formatted = []
    if not dates:
        return formatted
        
    for entry in dates:
        try:
            entry_dict = entry.asDict() if hasattr(entry, 'asDict') else entry
            raw_date = entry_dict.get("date", "")
            
            # Skip empty dates
            if not raw_date or raw_date.strip() == "":
                continue
                
            # Handle partial dates like "04/2025"
            if re.match(r"^\d{2}/\d{4}$", raw_date):
                raw_date = f"01/{raw_date}"  # Add day as 01
            
            parsed = None
            # Try parsing common formats
            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
                try:
                    parsed = datetime.strptime(raw_date, fmt)
                    break
                except Exception:
                    continue
                    
            if parsed:
                formatted_date = parsed.strftime("%d/%m/%Y")
                formatted.append({
                    "date": formatted_date,
                    "description": entry_dict.get("description", ""),
                    "comments": entry_dict.get("comments", "")
                })
        except Exception:
            continue
    return formatted


formatted_date_schema = ArrayType(StructType([
    StructField("date", StringType()),
    StructField("description", StringType()),
    StructField("comments", StringType())
]))

# Register and apply the UDF
standardize_dates_udf = udf(format_dates_to_ddmmyyyy, formatted_date_schema)


def normalize_text(text):
    if not text:
        return text
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text.strip()

normalize_text_udf = F.udf(normalize_text, StringType())

def normalize_title(text):
    if not text:
        return text
    # Lowercase and remove extra spaces
    text = text.lower().strip()
    text = re.sub(r'\s+', ' ', text)
    # Remove most punctuation except for basic readable ones (optional)
    text = re.sub(r'[^\w\s\-.,()]', '', text)
    # Capitalize first letter of each word (title case)
    return text.title()


normalize_title_udf = F.udf(normalize_title, StringType())


def standardize_countries(country_list):
    if not country_list:
        return country_list
    return [coco.convert(names=name, to='name_short') if name and name not in ["N/A", "All"] else name 
            for name in country_list]

countries_udf = F.udf(standardize_countries, ArrayType(StringType()))


def standardize_universities(uni_list):
    if not uni_list:
        return uni_list
    return [{
        "name": uni["name"] if "name" in uni else None,
        "country": coco.convert(names=uni["country"], to='name_short') if "country" in uni and uni["country"] else None
    } for uni in uni_list]

universities_udf = F.udf(standardize_universities, ArrayType(
    StructType([
        StructField("name", StringType()),
        StructField("country", StringType())
    ])
))


DEADLINE_KEYWORDS = ["deadline", "close", "submission"]

# UDF to compute status
def compute_status(dates):
    today = datetime.today()
    latest_relevant_date = None

    if not dates:
        return "unknown"

    for entry in dates:
        try:
            desc = entry.get("description", "").lower()
            if any(kw in desc for kw in DEADLINE_KEYWORDS):
                date_str = entry.get("date", "")
                # Expecting DD/MM/YYYY after standardization
                parsed_date = datetime.strptime(date_str, "%d/%m/%Y")
                if (latest_relevant_date is None) or (parsed_date > latest_relevant_date):
                    latest_relevant_date = parsed_date
        except Exception:
            continue

    if latest_relevant_date:
        return "closed" if latest_relevant_date < today else "open"
    return "unknown"

# Register and apply the UDF
status_udf = udf(compute_status, StringType())


def round_to_half(x):
    return round(x * 2) / 2

def normalize_duration(dur):
    if not dur:
        return dur
    dur = dur.lower().strip()
    
    # Match number followed by valid unit, including 'semester'
    m = re.match(r"(\d+(?:\.\d+)?)\s*(year|month|week|day|semester)s?", dur)
    if not m:
        if dur.isdigit():
            return dur  # just a number, assume years
        return dur
    
    num, unit = float(m.group(1)), m.group(2)
    
    if unit == "year":
        years = num
    elif unit == "semester":
        years = num * 0.5
    elif unit == "month":
        years = num / 12
    elif unit == "week":
        years = num / 52
    elif unit == "day":
        years = num / 365
    else:
        return dur
    
    return str(round_to_half(years))

duration_udf = F.udf(normalize_duration, StringType())

def normalize_admission(season):
    if not season:
        return season
    s = season.lower()
    if "fall" in s or "autumn" in s:
        return "fall"
    elif "spring" in s:
        return "spring"
    return season

admission_udf = F.udf(normalize_admission, StringType())

# Updated function to handle the new scholarship structure
def standardize_general_scholarship(scholarship_data_row):
    if not scholarship_data_row:
        return None
    
    standardized = {}
    scholarship_data = scholarship_data_row.asDict()
    
    # Handle scholarship_available_for_current_intake
    available_value = scholarship_data.get("scholarship_available_for_current_intake")
    if available_value:
        if available_value.lower().strip() == "no":
            standardized["scholarship_available_for_current_intake"] = False
        elif available_value.lower().strip() == "yes":
            standardized["scholarship_available_for_current_intake"] = True
        else:
            standardized["scholarship_available_for_current_intake"] = None
    else:
        standardized["scholarship_available_for_current_intake"] = None
    
    # Keep comments as is
    if "comments" in scholarship_data:
        standardized["comments"] = scholarship_data["comments"]
    
    # Handle tuition_coverage
    tuition_value = scholarship_data.get("tuition_coverage")
    if tuition_value and tuition_value != "N/A":
        if tuition_value.lower().strip() in ["yes", "true", "full"]:
            standardized["tuition_coverage"] = True
            standardized["tuition_coverage_details"] = None
        elif tuition_value.lower().strip() in ["no", "false"]:
            standardized["tuition_coverage"] = False
            standardized["tuition_coverage_details"] = None
        else:
            # If it's not a simple yes/no, store the details
            standardized["tuition_coverage"] = True  # Assume true if there are details
            standardized["tuition_coverage_details"] = str(tuition_value)
    else:
        standardized["tuition_coverage"] = None
        standardized["tuition_coverage_details"] = None
    
    # Handle travel_allowance
    travel_value = scholarship_data.get("travel_allowance")
    if travel_value and travel_value != "N/A":
        if travel_value.lower().strip() in ["yes", "true"]:
            standardized["travel_allowance"] = "yes"
        elif travel_value.lower().strip() in ["no", "false"]:
            standardized["travel_allowance"] = None
        else:
            standardized["travel_allowance"] = str(travel_value)
    else:
        standardized["travel_allowance"] = None
    
    # Handle monthly_allowance
    monthly_value = scholarship_data.get("monthly_allowance")
    if monthly_value and monthly_value != "N/A":
        if isinstance(monthly_value, str):
            # Extract numeric value from string like "£1,603"
            numbers = re.findall(r"\d+", monthly_value.replace(",", ""))
            if numbers:
                standardized["monthly_allowance"] = int(numbers[0])
            else:
                standardized["monthly_allowance"] = None
        elif isinstance(monthly_value, (int, float)):
            standardized["monthly_allowance"] = int(monthly_value)
        else:
            standardized["monthly_allowance"] = None
    else:
        standardized["monthly_allowance"] = None
    
    # Handle installation_costs
    installation_value = scholarship_data.get("installation_costs")
    if installation_value and installation_value != "N/A":
        if isinstance(installation_value, str):
            if installation_value.lower().strip() == "covered_in_monthly":
                standardized["installation_costs"] = "covered_in_monthly"
            else:
                # Extract numeric value
                numbers = re.findall(r"\d+", installation_value.replace(",", ""))
                if numbers:
                    standardized["installation_costs"] = int(numbers[0])
                else:
                    standardized["installation_costs"] = None
        elif isinstance(installation_value, (int, float)):
            standardized["installation_costs"] = int(installation_value)
        else:
            standardized["installation_costs"] = None
    else:
        standardized["installation_costs"] = None
    
    return standardized if standardized else None

# Updated schema for general scholarship structure
general_scholarship_schema = StructType([
    StructField("scholarship_available_for_current_intake", BooleanType(), True),
    StructField("comments", StringType(), True),
    StructField("tuition_coverage", BooleanType(), True),
    StructField("tuition_coverage_details", StringType(), True),
    StructField("travel_allowance", StringType(), True),
    StructField("monthly_allowance", IntegerType(), True),
    StructField("installation_costs", StringType(), True)
])

# Register the UDF
standardize_general_scholarship_udf = F.udf(standardize_general_scholarship, general_scholarship_schema)

# Load Data 
df = spark.read.json(f"{PATH}/structured_scholarships.json", multiLine=True)

df_clean = (
    df
    .withColumn("clean_scholarship_name", normalize_title_udf(F.col("extracted.scholarship_name")))
    .withColumn("clean_description", normalize_text_udf(F.col("extracted.description")))
    .withColumn("clean_important_dates", standardize_dates_udf(col("extracted.important_dates")))
    .withColumn("clean_duration", duration_udf(F.col("extracted.duration")))
    .withColumn("clean_admission", admission_udf(F.col("extracted.admission")))
    .withColumn("clean_universities", universities_udf(F.col("extracted.universities")))
    .withColumn("clean_scholarship", standardize_general_scholarship_udf(F.col("extracted.scholarship")))
)

df_clean = df_clean.withColumn("clean_status", status_udf(col("clean_important_dates")))


output_path = f"{PATH}/clean_uk.json"
with open(output_path, "w", encoding="utf-8") as f:
    for row in df_clean.toJSON().collect():
        f.write(row + "\n")

print("Cleaned JSONs saved ' ")

# Define your S3 paths
S3_BUCKET = "scholamigo"
S3_PREFIX = "trusted_zone_data/uk_data/"

# Write directly to S3 in Parquet format with schema preservation
df_clean.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .parquet(f"s3a://{S3_BUCKET}/{S3_PREFIX}clean_uk.parquet")

print(f"Data saved to S3: s3://{S3_BUCKET}/{S3_PREFIX}clean_uk.parquet")