import os
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from bs4 import BeautifulSoup
import google.generativeai as genai
import json
import re
from pathlib import Path

from time import sleep

aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
genai_api_key = os.getenv('GenAI_API_KEY')

# local_parquet_path = "file:///Users/marwasulaiman/Documents/BDMA/UPC%20-%202nd%20semester/BDM/Project/erasmus_data.parquet"

output_dir = "trusted_zone/data/erasmus_data"

Path(output_dir).mkdir(parents=True, exist_ok=True)

# Create S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

# --- S3 Configuration ---
bucket_name = 'scholamigo'
prefix = 'landing_zone_data/erasmus_data/' 

genai.configure(api_key=genai_api_key)


spark = SparkSession.builder \
    .appName("ReadErasmusFromS3") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.665") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.network.timeout", "300s") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .getOrCreate()


# retrieve all files 
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
all_files = [f"s3a://{bucket_name}/{obj['Key']}" for obj in response.get('Contents', []) if obj['Key'].endswith('.json')]
df = spark.read.option("multiLine", True).json(all_files)

print("Created DF!")

# df = spark.read.parquet(local_parquet_path)


# UDF to extract structured sections from HTML
def extract_sections(html):
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")

    for tag in soup.find_all(["nav", "footer", "aside", "script", "style"]):
        tag.decompose()

    for class_name in ["menu", "footer", "navbar", "sidebar"]:
        for tag in soup.find_all(class_=class_name):
            tag.decompose()

    result = []
    current_section = {"heading": "Introduction", "content": ""}

    section_tags = ["h1", "h2", "h3", "h4", "h5", "h6", "p", "div", "ul", "ol"]

    for tag in soup.find_all(section_tags):
        parent = tag.find_parent(section_tags)
        if parent is not None:
            continue  # Skip nested tags

        if tag.name in ["h1", "h2", "h3", "h4", "h5", "h6"]:
            if current_section["content"].strip():
                result.append(current_section)
            current_section = {"heading": tag.get_text(strip=True), "content": ""}
        else:
            current_section["content"] += " " + tag.get_text(strip=True)

    if current_section["content"].strip():
        result.append(current_section)

    return result

# Register UDF
section_schema = ArrayType(
    StructType([
        StructField("heading", StringType(), True),
        StructField("content", StringType(), True)
    ])
)
extract_sections_udf = udf(extract_sections, section_schema)

# Flatten all scholarships
df_flat = df.select(
    col("url").alias("top_level_url"),
    explode(col("pages")).alias("page"),
    col("titles")
)

df_pages = df_flat.select(
    "top_level_url",
    "titles",
    col("page.html").alias("page_html"),
    col("page.url").alias("page_url")
)

# Apply HTML section extraction
df_structured = df_pages.withColumn("sections", extract_sections_udf("page_html")).drop("page_html")

# Convert to JSON rows (as list of strings)
json_list = df_structured.toJSON().collect()

# Group rows by scholarship (top_level_url)
from collections import defaultdict
grouped_by_url = defaultdict(list)

for row in json_list:
    data = json.loads(row)
    grouped_by_url[data["top_level_url"]].append(data)

# Load Gemini model
model = genai.GenerativeModel("gemini-1.5-flash-latest")

# Run generation for each scholarship
final_outputs = []

for url, pages in grouped_by_url.items():
    print(url)
    # sleep(10)
    scholarship_text = json.dumps(pages, ensure_ascii=False)
    # prompt = prompt_template.replace("{scholarship_text}", scholarship_text)
    prompt = f"""
You are an expert data extraction assistant for scholarship databases.

Your task is to extract specific structured information in **valid JSON format only** (without markdown, explanations, or extra text). Always follow the exact structure below. If any field is missing or not mentioned, use `"N/A"` (as a string) or empty lists (`[]`) where appropriate.

Ensure the output is always a **valid JSON object** using the following structure:

{{
  "scholarship_name": "",
  "description": "",
  "important_dates": [{{"date": "strictly DD/MM/YYYY", "description": "application_opening or application_deadline", "comments":""}}], //Deadlines and opening date
  "deadlines_more_details_link": "",
  "countries": [],  // only include the countrie where the study is held, and according English short name (like 'Egypt', 'Pakistan', 'Spain',..)
  "scholarship_type": "",  // One of "full", "partial", "no_funding", or "full or self_funded"
  "erasmus+_scholarship": (specify the amounts as mentioned in the text, but in standard format (amount + currency symbol + strictly specify 'per year'/'per month'/'per semester'/'in one payment' + strictly any comments between parenthesis)
    {{
    "scholarship_available_for_current_intake: (Yes/No)
    "comments": "

    if we have differences between partner and programme countries:
    "erasmus+_scholarship_partner_countries_scholars": {{
      "tuition_coverage": "", // tuition fees covered for partner countries scholars (if numbers are not stated, write 'Yes' to indicate coverage or 'No' to indicate no coverage)
      "travel_allowance": "", // allowance for travel for partner countries scholars (if it is included in the monthly allowance, write "covered by monthly allowance")
      "monthly_allowance": "", // monthly stipend for partner countries scholars
      "installation_costs": "" // installation costs at the beginning for partner countries scholars
    }},
    "erasmus+_scholarship_programme_countries_scholars": {{
      "tuition_coverage": "", // tuition fees covered for programme countries scholars (if numbers are not stated, write 'Yes' to indicate coverage or 'No' to indicate no coverage)
      "travel_allowance": "", // allowance for travel for programme countries scholars (if it is included in the monthly allowance, write "covered by monthly allowance")
      "monthly_allowance": "", // monthly stipend for programme countries scholars
      "installation_costs": "" // installation costs at the beginning for programme countries scholars
    }},

    if we don't have differences between partner and programme countries:
    "erasmus+_scholarship_general_scholars": {{
      "tuition_coverage": "",
      "travel_allowance": "",
      "monthly_allowance": "",
      "installation_costs": ""
    }},
  }},
  "other_scholarships": "",
  "scholarship_more_details_link": "",
  "requirements": {{
    "language_proficiency": //include all required languages mentioned  
    [{{
      "language": "",
      "min_level": "", //e.g. B1, B2
      "exams": [{{"name": "", "min_score": ""}}],
      "exemption_cases": ""
    }}],
    "gender": "",  // "F", "M", or "Any"
    "nationality": "",  // English short name or "All"
    "GPA": "",
    "other_exams": "",  // e.g., SAT, GRE
    "bachelor_requirement": "",  // field and min ECTS if any
    "letters_of_recommendation": "",
    "motivation_letter": "",  // "Yes" or "No"
    "requirements_more_details_link": ""
  }},
  "required_documents": "",
  "level": "",  // "Bachelor", "Master", or "PhD"
  "fields_of_study": [], // only limited to the 10 standard fields in ISCED-F 2013 ( Generic programmes and qualifications, Education, Arts and humanities, Social sciences, journalism and information
                            , Business, administration and law, Natural sciences, mathematics and statistics, Information and Communication Technologies
                            , Engineering, manufacturing and construction, Agriculture, forestry, fisheries and veterinary, Health and welfare, Services)
  "course_topics": [],
  "courses_details_link": "",
  "mobility_structure_details_link": "",
  "universities": [{{"name": "", "country": ""}}],
  "admission": "",  // "fall", "spring", or "N/A"
  "website": "",
  "duration": "",  // e.g., "2 years" (fixed format of number followed by "months" or "years") / also, if there is an internship or thesis after third semester count it as fourth semester.
  "Other": ["","",..]  // relevant and important information not mentioned above (ex. info about fees coverage, internships, summer school, special procedures, other benefits like health insurance etc)
}}

Now extract the above fields from the following scholarship description. Always preserve this structure, and return only the JSON object.

Scholarship description:
{scholarship_text}
""".strip()

    try:
        response = model.generate_content(prompt)
        match = re.search(r"\{.*\}", response.text, re.DOTALL)
        if match:
            json_str = match.group(0)
            data = json.loads(json_str)
            final_outputs.append({"url": url, "extracted": data})
            print(f"JSON of {url} Done")
            sleep(15)
        else:
            print(f"No JSON found for {url}")
    except Exception as e:
        print(f"Error processing {url}: {e}")

# Optional: Save output to file
with open(f"{output_dir}/structured_scholarships.json", "w", encoding="utf-8") as f:
    json.dump(final_outputs, f, ensure_ascii=False, indent=2)

print(f"Extracted {len(final_outputs)} scholarships")


