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

PATH = "trusted_zone/data/erasmus_data"

local_parquet_path = "trusted_zone/data/erasmus_data/clean_erasmus.parquet"

aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')

spark = SparkSession.builder \
    .appName("ReadErasmusFromS3") \
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
    for entry in dates:
        # print(entry)
        try:
            entry_dict = entry.asDict()  # Convert Row to dict
            raw_date = entry_dict.get("date", "")
            # if "YYYY" in raw_date:
            #     raw_date = raw_date.replace("YYYY", "2025")

            # parsed = None
            # Try parsing common formats
            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
                try:
                    parsed = datetime.strptime(raw_date, fmt)
                    break
                except Exception:
                    parsed = None
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

# TODO: currency and amounts standardization / check on standard fields

def standardize_erasmus_scholarship(erasmus_data_row):
    # print("inputtttt:", erasmus_data) 

    if not erasmus_data_row:
        return None
    
    standardized = {}

    erasmus_data = erasmus_data_row.asDict()
    
    keys_to_check = [
        "scholarship_available_for_current_intake",
        "scholarship_available_for_current_intake:"
    ]

    value = None

    for key in keys_to_check:
        if key in erasmus_data and erasmus_data[key] and str(erasmus_data[key]).strip().lower() != "none":
            # print(key)
            value = erasmus_data[key]

    # Remove both keys
    for key in keys_to_check:
        erasmus_data.pop(key, None)

    # Insert the clean key if a value was found
    if value is not None:
        if value.lower().strip()=="no":
            standardized["scholarship_available_for_current_intake"] = False
        elif value.lower().strip()=="yes":
            standardized["scholarship_available_for_current_intake"] = True
    else:
        standardized["scholarship_available_for_current_intake"] = None
    

    # Keep comments as is
    if isinstance(erasmus_data, dict) and "comments" in erasmus_data:
        standardized["comments"] = erasmus_data["comments"]
    
    # Handle programme and partner countries
    if isinstance(erasmus_data, dict):
        for country_type in ["programme_countries", "partner_countries"]:
            country_key = f"erasmus+_scholarship_{country_type}_scholars"
            general_key = "erasmus+_scholarship_general_scholars"
            
            # Prefer the specific country type data over general data
            country_data = None
            if country_key in erasmus_data and erasmus_data[country_key] is not None:
                country_data = erasmus_data[country_key].asDict()
            elif general_key in erasmus_data and erasmus_data[general_key] is not None:
                country_data = erasmus_data[general_key].asDict()
                
            if not country_data or not isinstance(country_data, dict):
                continue
                
            standardized_country = {}
            
            # Handle tuition_coverage
            if "tuition_coverage" in country_data:
                value = country_data["tuition_coverage"]
                
                if value.lower().strip() in ["yes", "true", "no", "false"]:
                    standardized_country["tuition_coverage_details"] = None
                else:
                    standardized_country["tuition_coverage_details"] = str(value)  # Preserve original details
                
                if isinstance(value, str):
                    value = value.lower().strip()
                    # Check for any numeric value in the string (like "9000€")
                    has_numeric = any(c.isdigit() for c in value)
                    
                    if value in ["yes", "full", "partial", "true"]:
                        standardized_country["tuition_coverage"] = True
                    elif value in ["no", "false"]:
                        standardized_country["tuition_coverage"] = False
                    elif has_numeric:  # If string contains any numbers (like "9000€")
                        standardized_country["tuition_coverage"] = True
                    else:
                        standardized_country["tuition_coverage"] = None 
                elif isinstance(value, (int, float)):
                    standardized_country["tuition_coverage"] = True if value > 0 else False
                else:
                    standardized_country["tuition_coverage"] = bool(value) if value is not None else None
            else:
                standardized_country["tuition_coverage"] = None
                standardized_country["tuition_coverage_details"] = None
                
            # Handle travel_allowance
            if "travel_allowance" in country_data:
                value = country_data["travel_allowance"]
                if isinstance(value, str):
                    value = value.lower().strip()
                    if "covered by monthly allowance" in value:
                        standardized_country["travel_allowance"] = "covered_in_monthly"
                    elif "conditional" in value or "km" in value:
                        # # Try to parse conditional based on distance
                        # numbers = re.findall(r"\d+", value)
                        # if len(numbers) >= 2:
                        #     amounts = [int(n) for n in numbers[:3]]
                        #     distances = ["<4000", ">=4000"]
                        #     standardized_country["travel_allowance"] = [
                        #         {"amount": amounts[0], "distance": distances[0]},
                        #         {"amount": amounts[2], "distance": distances[1]}
                        #     ]
                        standardized_country["travel_allowance"] = value.replace("\n",", ")
                        # else:
                        #     standardized_country["travel_allowance"] = None
                    elif value in ["n/a", "none", ""]:
                        standardized_country["travel_allowance"] = None
                    elif value.replace(".", "").isdigit():  # Simple numeric value
                        standardized_country["travel_allowance"] = int(value.replace(".", ""))
                    else:
                        standardized_country["travel_allowance"] = None
                elif isinstance(value, (int, float)):
                    standardized_country["travel_allowance"] = int(value)
                else:
                    standardized_country["travel_allowance"] = None
            else:
                standardized_country["travel_allowance"] = None

            # Handle monthly_allowance
            if "monthly_allowance" in country_data:
                value = country_data["monthly_allowance"]
                if isinstance(value, str):
                    value = value.lower().strip()
                    if value in ["n/a", "none", ""]:
                        standardized_country["monthly_allowance"] = None
                    else:
                        # Extract numeric value
                        numbers = re.findall(r"\d+", value.replace(".","").replace(",",""))
                        if numbers:
                            standardized_country["monthly_allowance"] = int(numbers[0])
                        else:
                            standardized_country["monthly_allowance"] = None
                elif isinstance(value, (int, float)):
                    standardized_country["monthly_allowance"] = int(value)
                else:
                    standardized_country["monthly_allowance"] = None
            else:
                standardized_country["monthly_allowance"] = None
            
            # Handle installation_costs
            if "installation_costs" in country_data:
                value = country_data["installation_costs"]
                if isinstance(value, str):
                    value = value.lower().strip()
                    if value == "covered_in_monthly":
                        standardized_country["installation_costs"] = "covered_in_monthly"
                    elif value in ["n/a", "none", ""]:
                        standardized_country["installation_costs"] = None
                    else:
                        # Extract numeric value
                        numbers = re.findall(r"\d+", value.replace(".","").replace(",",""))
                        if numbers:
                            standardized_country["installation_costs"] = int(numbers[0])
                        else:
                            standardized_country["installation_costs"] = None
                elif isinstance(value, (int, float)):
                    standardized_country["installation_costs"] = int(value)
                else:
                    standardized_country["installation_costs"] = None
            else:
                standardized_country["installation_costs"] = None

            if standardized_country:
                standardized[country_type] = standardized_country
    
    return standardized if standardized else None

# Define the schema for the nested structure
scholarship_schema = StructType([
    StructField("scholarship_available_for_current_intake", BooleanType(), True),
    StructField("comments", StringType(), True),
    StructField("programme_countries", StructType([
        StructField("tuition_coverage", BooleanType(), True),
        StructField("tuition_coverage_details", StringType(), True),
        StructField("travel_allowance", StringType(), True),
        StructField("monthly_allowance", IntegerType(), True),
        StructField("installation_costs", StringType(), True)
    ]), True),
    StructField("partner_countries", StructType([
        StructField("tuition_coverage", BooleanType(), True),
        StructField("tuition_coverage_details", StringType(), True),
        StructField("travel_allowance", StringType(), True),
        StructField("monthly_allowance", IntegerType(), True),
        StructField("installation_costs", StringType(), True)
    ]), True)
])

# Register the UDF
standardize_erasmus_udf = F.udf(standardize_erasmus_scholarship, scholarship_schema)

# Load Data 
df = spark.read.json(f"{PATH}/structured_scholarships.json", multiLine=True)

df_clean = (
    df
    .withColumn("clean_scholarship_name", normalize_title_udf(F.col("extracted.scholarship_name")))
    .withColumn("clean_description", normalize_text_udf(F.col("extracted.description")))
    .withColumn("clean_important_dates", standardize_dates_udf(col("extracted.important_dates")))
    .withColumn("clean_duration", duration_udf(F.col("extracted.duration")))
    .withColumn("clean_admission", admission_udf(F.col("extracted.admission")))
    .withColumn("clean_countries", countries_udf(F.col("extracted.countries")))
    .withColumn("clean_universities", universities_udf(F.col("extracted.universities")))
    .withColumn("clean_erasmus_scholarship", standardize_erasmus_udf(F.col("extracted.erasmus+_scholarship")))

)

df_clean = df_clean.withColumn("clean_status", status_udf(col("clean_important_dates")))


# df_clean.write.mode("overwrite").parquet(local_parquet_path)

# output_path = f"{PATH}/clean_erasmus.json"
# with open(output_path, "w", encoding="utf-8") as f:
#     for row in df_clean.toJSON().collect():
#         f.write(row + "\n")

# print("Cleaned JSONs saved to 'clean_erasmus.json' and 'clean_erasmus.parquet' ")


# Define your S3 paths
S3_BUCKET = "scholamigo"
S3_PREFIX = "trusted_zone_data/erasmus_data/"

# Write directly to S3 in Parquet format with schema preservation
df_clean.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .parquet(f"s3a://{S3_BUCKET}/{S3_PREFIX}clean_erasmus.parquet")

# Write directly to S3 in JSON format (as individual files)
# df_clean.write.mode("overwrite").json(f"s3a://{S3_BUCKET}/{S3_PREFIX}clean_erasmus.json")

print(f"Data saved to S3: s3://{S3_BUCKET}/{S3_PREFIX}clean_erasmus.parquet")