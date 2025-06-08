#%%

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

#%%


aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
genai_api_key = os.getenv('GenAI_API_KEY')


# local_parquet_path = "file:///Users/marwasulaiman/Documents/BDMA/UPC%20-%202nd%20semester/BDM/Project/erasmus_data.parquet"

output_dir = "trusted_zone/data/uk_data"

Path(output_dir).mkdir(parents=True, exist_ok=True)

# Create S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

# --- S3 Configuration ---
bucket_name = 'scholamigo'
prefix = 'landing_zone_data/uk_data/' 

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



# # Path to your local JSON file
# local_path = "/Users/marwasulaiman/Documents/BDMA/UPC - 2nd semester/BDM/Project/ScholAmigo_BDM/landing_zone/data/uk_data/program_manchester_ac_uk_1.json"

# # Load the local JSON into a DataFrame
# df = spark.read.option("multiLine", True).json(local_path)

# # S3 destination path
s3_path = f"s3a://{bucket_name}/{prefix}program_manchester_ac_uk_1.json"

# # Write DataFrame to S3
# df.write.mode("overwrite").json(s3_path)

#%%

df = spark.read.option("multiLine", True).json(s3_path)

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
    explode(col("pages")).alias("page")
)

df_pages = df_flat.select(
    "top_level_url",
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

    # with open(f"{output_dir}/scholarship_text.txt", "w") as file:
    #     file.write(scholarship_text)    

    prompt = f"""
You are an expert data extraction assistant for scholarship databases.

Your task is to extract specific structured information and return it as a **valid JSON array only** (without markdown, explanations, or extra text).

**IMPORTANT**: 
- If there is only ONE scholarship, return an array with one object: `[{{scholarship_object}}]`
- If there are MULTIPLE scholarships, return an array with multiple objects: `[{{scholarship1}}, {{scholarship2}}, ...]`
- Always return a JSON array, even for single scholarships
- Do not include any text before or after the JSON array

Each scholarship object in the array should follow this exact structure. If any field is missing or not mentioned, use `"N/A"` (as a string) or empty lists (`[]`) where appropriate:

{{
  "scholarship_name": "", (ex. Humanitarian Scholarship)
  "description": "",
  "important_dates": [{{"date": "strictly DD/MM/YYYY", "description": "application_opening or application_deadline", "comments":""}}], //Deadlines and opening date
  "deadlines_more_details_link": "",
  "scholarship_type": "",  // One of "full", "partial", "no_funding", or "full or self_funded"
  "scholarship": {{
    "scholarship_available_for_current_intake": "", // (Yes/No)
    "comments": "",
    "tuition_coverage": "",  // tuition fees covered for partner countries scholars (if numbers are not stated, write 'Yes' to indicate coverage or 'No' to indicate no coverage)
    "travel_allowance": "", // allowance for travel for partner countries scholars (if it is included in the monthly allowance, write "covered by monthly allowance")
    "monthly_allowance": "", // monthly stipend for partner countries scholars (specify amounts as mentioned, in standard format)
    "installation_costs": "" // installation costs at the beginning for partner countries scholars
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
  "fields_of_study": [], // only limited to the 10 standard fields in ISCED-F 2013 ( Generic programmes and qualifications, Education, Arts and humanities, Social sciences, journalism and information, Business, administration and law, Natural sciences, mathematics and statistics, Information and Communication Technologies, Engineering, manufacturing and construction, Agriculture, forestry, fisheries and veterinary, Health and welfare, Services)
  "course_topics": [],
  "courses_details_link": "",
  "universities": [{{"name": "", "country": ""}}],
  "admission": "",  // "fall", "spring", or "N/A"
  "website": "",
  "duration": "",  // e.g., "2 years" (fixed format of number followed by "months" or "years") / also, if there is an internship or thesis after third semester count it as fourth semester.
  "Other": ["","",..]  // relevant and important information not mentioned above (ex. info about fees coverage, internships, summer school, special procedures, other benefits like health insurance etc)
}}

Scholarship description:
{scholarship_text}
""".strip()

    # response = model.generate_content(prompt)

    # with open(f"{output_dir}/response.txt", "w") as file:
    #     file.write(response.text)    

#%%
    try:
        response = model.generate_content(prompt)
        # Look for JSON array in the response
        match = re.search(r'\[.*\]', response.text, re.DOTALL)
        if match:
            json_str = match.group(0)

            # It only works this way!!
            with open(f"{output_dir}/response.txt", "w") as file:
                file.write(json_str)    

            with open(f"{output_dir}/response.txt", 'r', encoding='utf-8') as file:
                content = file.read()

            data_array = json.loads(content)

            # Add each scholarship to final outputs with the URL
            for scholarship_data in data_array:
                final_outputs.append({"url": url, "extracted": scholarship_data})
            
            print(f"Extracted {len(data_array)} scholarship(s) from {url}")
            sleep(15)
        else:
            print(f"No JSON array found for {url}")
            print(f"Response was: {response.text[:200]}...")
    except Exception as e:
        print(f"Error processing {url}: {e}")

# Optional: Save output to file
with open(f"{output_dir}/structured_scholarships.json", "w", encoding="utf-8") as f:
    json.dump(final_outputs, f, ensure_ascii=False, indent=2)

print(f"Extracted {len(final_outputs)} scholarships total")