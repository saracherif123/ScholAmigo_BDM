import os
import json
import re
import boto3
from typing import Dict, Any, List
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, when, trim, udf
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, FloatType, ArrayType
from botocore.exceptions import ClientError, NoCredentialsError

# S3 Configuration
S3_BUCKET = "scholamigo"
LANDING_ZONE_PREFIX = "landing_zone_data"
TRUSTED_ZONE_PREFIX = "trusted_zone_data/student_alumni_data"

# Country mapping for standardization
COUNTRY_MAPPING = {
    'usa': 'united states of america',
    'us': 'united states of america',
    'uk': 'united kingdom',
    'uae': 'united arab emirates',
    'canada': 'canada',
    'germany': 'germany',
    'france': 'france',
    'spain': 'spain',
    'italy': 'italy',
    'portugal': 'portugal',
    'slovenia': 'slovenia',
    'vietnam': 'vietnam',
    'singapore': 'singapore'
}

def initialize_spark():
    """Initialize Spark session with appropriate configurations for JDK 11 and S3"""
    spark = SparkSession.builder \
        .appName("StudentDataPreprocessing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()
    
    return spark

def initialize_s3_client():
    """Initialize S3 client with explicit credentials from environment variables"""
    try:
        # Load credentials from environment variables
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')  # Default to us-east-1 if not set
        
        # Check if credentials are available
        if not aws_access_key or not aws_secret_key:
            raise ValueError("AWS credentials not found in environment variables. "
                           "Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
        
        print(f"Loading AWS credentials from environment variables...")
        print(f"Region: {aws_region}")
        
        # Create S3 client with explicit credentials
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )
        
        # Test connection by checking bucket access
        s3_client.head_bucket(Bucket=S3_BUCKET)
        print(f"Successfully connected to S3 bucket: {S3_BUCKET}")
        return s3_client
        
    except ValueError as e:
        print(f"Credential Error: {e}")
        raise
    except NoCredentialsError:
        print("Error: AWS credentials not found. Please configure your AWS credentials.")
        raise
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"Error: S3 bucket '{S3_BUCKET}' not found.")
        elif error_code == '403':
            print(f"Error: Access denied to S3 bucket '{S3_BUCKET}'. Check your credentials and permissions.")
        else:
            print(f"Error accessing S3 bucket: {e}")
        raise

def read_s3_file(s3_client, bucket, key):
    """Read file from S3 bucket"""
    try:
        print(f"Reading file from S3: s3://{bucket}/{key}")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        print(f"Successfully read file: {key}")
        return content
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            print(f"Error: File '{key}' not found in bucket '{bucket}'")
        else:
            print(f"Error reading file from S3: {e}")
        raise

def write_s3_file(s3_client, bucket, key, content):
    """Write file to S3 bucket"""
    try:
        print(f"Writing file to S3: s3://{bucket}/{key}")
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content,
            ContentType='application/json'
        )
        print(f"Successfully wrote file: {key}")
    except ClientError as e:
        print(f"Error writing file to S3: {e}")
        raise

def list_s3_files(s3_client, bucket, prefix):
    """List files in S3 bucket with given prefix"""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents']]
        else:
            return []
    except ClientError as e:
        print(f"Error listing files in S3: {e}")
        return []

def find_s3_file_by_pattern(s3_client, bucket, prefix, pattern):
    """Find S3 file matching a pattern"""
    files = list_s3_files(s3_client, bucket, prefix)
    for file_key in files:
        if pattern in file_key:
            return file_key
    return None

def clean_text(text):
    """Clean text by removing weird characters, extra spaces, and newlines"""
    if text is None:
        return None
    
    # Convert to string if not already
    text = str(text)
    
    # Remove newlines and excessive whitespace
    text = re.sub(r'\n+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    
    # Remove weird characters but keep basic punctuation
    text = re.sub(r'[^\w\s.,!?;:()\-\'"]', '', text)
    
    # Remove "see more" artifacts
    text = re.sub(r'…see more', '', text)
    
    # Strip leading/trailing whitespace
    text = text.strip()
    
    return text if text else None

def standardize_country(country):
    """Standardize country names to international standard full names"""
    if country is None:
        return None
    
    country_lower = str(country).lower().strip()
    return COUNTRY_MAPPING.get(country_lower, country_lower)

def ensure_non_negative(value):
    """Ensure numeric values are non-negative"""
    if value is None:
        return None
    try:
        num_val = float(value)
        return max(0, num_val)
    except (ValueError, TypeError):
        return None

def process_student_profiles(s3_client, bucket, file_key, spark):
    """Process student profiles data from S3"""
    print(f"Processing student profiles from S3: {file_key}")
    
    # Read the JSONL file from S3
    content = read_s3_file(s3_client, bucket, file_key)
    
    # Parse JSONL content
    data = []
    for line in content.strip().split('\n'):
        if line.strip():
            try:
                data.append(json.loads(line.strip()))
            except json.JSONDecodeError as e:
                print(f"Warning: Skipping invalid JSON line: {e}")
                continue
    
    if not data:
        print("No valid data found in student profiles file")
        return []
    
    # Create UDFs for cleaning functions
    clean_text_udf = udf(clean_text, StringType())
    standardize_country_udf = udf(standardize_country, StringType())
    ensure_non_negative_udf = udf(ensure_non_negative, FloatType())
    
    # Convert to DataFrame
    df = spark.createDataFrame(data)
    
    # Apply transformations
    df_cleaned = df.select(
        lower(clean_text_udf(col("Name"))).alias("name"),
        ensure_non_negative_udf(col("Age")).alias("age"),
        lower(clean_text_udf(col("Sex"))).alias("sex"),
        lower(clean_text_udf(col("Major"))).alias("major"),
        lower(clean_text_udf(col("Year"))).alias("year"),
        ensure_non_negative_udf(col("GPA")).alias("gpa"),
        col("Hobbies").alias("hobbies"),  # Keep as array, will clean individual items
        standardize_country_udf(col("Country")).alias("country"),
        lower(clean_text_udf(col("State/Province"))).alias("state_province"),
        lower(clean_text_udf(col("Unique Quality"))).alias("unique_quality"),
        clean_text_udf(col("Story")).alias("story")
    )
    
    # Handle hobbies array - clean each hobby
    def clean_hobbies_array(hobbies):
        if hobbies is None:
            return None
        return [clean_text(hobby).lower() if hobby else None for hobby in hobbies]
    
    clean_hobbies_udf = udf(clean_hobbies_array, ArrayType(StringType()))
    df_cleaned = df_cleaned.withColumn("hobbies", clean_hobbies_udf(col("hobbies")))
    
    # Handle name deduplication
    name_counts = df_cleaned.groupBy("name").count().collect()
    name_count_dict = {row['name']: row['count'] for row in name_counts}
    
    def deduplicate_name(name):
        if name_count_dict.get(name, 1) > 1:
            # This is a simplified approach - in a real scenario, you'd want more sophisticated deduplication
            import random
            return f"{name}_{random.randint(1, 1000)}"
        return name
    
    deduplicate_name_udf = udf(deduplicate_name, StringType())
    df_cleaned = df_cleaned.withColumn("name", deduplicate_name_udf(col("name")))
    
    # Convert back to list of dictionaries
    cleaned_data = [row.asDict() for row in df_cleaned.collect()]
    
    return cleaned_data

def process_alumni_profiles(s3_client, bucket, file_key, spark):
    """Process alumni LinkedIn profiles data from S3"""
    print(f"Processing alumni profiles from S3: {file_key}")
    
    # Read the JSON file from S3
    content = read_s3_file(s3_client, bucket, file_key)
    
    # Parse JSON content
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON content: {e}")
        return {}
    
    def clean_nested_data(data_dict):
        """Recursively clean nested dictionary/list structures"""
        if isinstance(data_dict, dict):
            cleaned = {}
            for key, value in data_dict.items():
                if isinstance(value, str):
                    cleaned[key.lower()] = clean_text(value)
                elif isinstance(value, list):
                    cleaned[key.lower()] = [clean_nested_data(item) for item in value]
                elif isinstance(value, dict):
                    cleaned[key.lower()] = clean_nested_data(value)
                else:
                    cleaned[key.lower()] = value
            return cleaned
        elif isinstance(data_dict, list):
            return [clean_nested_data(item) for item in data_dict]
        elif isinstance(data_dict, str):
            return clean_text(data_dict)
        else:
            return data_dict
    
    # Clean the entire data structure
    cleaned_data = clean_nested_data(data)
    
    # Handle name deduplication for alumni profiles
    all_names = []
    for org_key in cleaned_data:
        if 'profiles' in cleaned_data[org_key]:
            for profile in cleaned_data[org_key]['profiles']:
                if 'name' in profile:
                    all_names.append(profile['name'])
    
    # Count name occurrences
    name_counts = {}
    for name in all_names:
        name_counts[name] = name_counts.get(name, 0) + 1
    
    # Deduplicate names
    name_suffix_counters = {}
    for org_key in cleaned_data:
        if 'profiles' in cleaned_data[org_key]:
            for profile in cleaned_data[org_key]['profiles']:
                if 'name' in profile:
                    original_name = profile['name']
                    if name_counts.get(original_name, 1) > 1:
                        if original_name not in name_suffix_counters:
                            name_suffix_counters[original_name] = 1
                        else:
                            name_suffix_counters[original_name] += 1
                        profile['name'] = f"{original_name}_{name_suffix_counters[original_name]}"
    
    return cleaned_data

def save_cleaned_data_to_s3(s3_client, bucket, key, data, file_format='json'):
    """Save cleaned data to S3"""
    print(f"Saving cleaned data to S3: s3://{bucket}/{key}")
    
    if file_format == 'jsonl':
        # Create JSONL content
        lines = []
        for item in data:
            lines.append(json.dumps(item, ensure_ascii=False))
        content = '\n'.join(lines)
    else:
        # Create JSON content
        content = json.dumps(data, indent=2, ensure_ascii=False)
    
    write_s3_file(s3_client, bucket, key, content)
    print(f"Data saved successfully to s3://{bucket}/{key}")

def main():
    # Initialize S3 client
    try:
        s3_client = initialize_s3_client()
    except Exception as e:
        print(f"Failed to initialize S3 client: {e}")
        return
    
    # Initialize Spark
    spark = initialize_spark()
    
    try:
        # S3 file paths
        student_profiles_key = find_s3_file_by_pattern(
            s3_client, S3_BUCKET, 
            f"{LANDING_ZONE_PREFIX}/users_data/", 
            "student_profiles.jsonl"
        )
        
        alumni_profiles_key = find_s3_file_by_pattern(
            s3_client, S3_BUCKET, 
            f"{LANDING_ZONE_PREFIX}/erasmus_linkedin_profiles/", 
            "Erasmus_Linkedin_Profiles.json"
        )
        
        # Output S3 keys
        cleaned_student_profiles_key = f"{TRUSTED_ZONE_PREFIX}/cleaned_student_profiles.jsonl"
        cleaned_alumni_profiles_key = f"{TRUSTED_ZONE_PREFIX}/cleaned_alumni_profiles.json"
        
        # Process student profiles
        if student_profiles_key:
            cleaned_student_data = process_student_profiles(s3_client, S3_BUCKET, student_profiles_key, spark)
            print(f"Processed {len(cleaned_student_data)} student profiles")
        else:
            print(f"Warning: student_profiles.jsonl not found in s3://{S3_BUCKET}/{LANDING_ZONE_PREFIX}/users_data/")
            cleaned_student_data = []
        
        # Process alumni profiles
        if alumni_profiles_key:
            cleaned_alumni_data = process_alumni_profiles(s3_client, S3_BUCKET, alumni_profiles_key, spark)
            total_alumni = sum(len(org_data.get('profiles', [])) for org_data in cleaned_alumni_data.values())
            print(f"Processed {total_alumni} alumni profiles")
        else:
            print(f"Warning: Erasmus_Linkedin_Profiles.json not found in s3://{S3_BUCKET}/{LANDING_ZONE_PREFIX}/erasmus_linkedin_profiles/")
            cleaned_alumni_data = {}
        
        # Save cleaned data to S3
        if cleaned_student_data:
            save_cleaned_data_to_s3(s3_client, S3_BUCKET, cleaned_student_profiles_key, cleaned_student_data, 'jsonl')
        
        if cleaned_alumni_data:
            save_cleaned_data_to_s3(s3_client, S3_BUCKET, cleaned_alumni_profiles_key, cleaned_alumni_data, 'json')
        
        print("\nData preprocessing completed successfully!")
        
        # Print sample of cleaned data for verification
        if cleaned_student_data:
            print("\nSample cleaned student profile:")
            print(json.dumps(cleaned_student_data[0], indent=2))
        
        if cleaned_alumni_data:
            print("\nSample cleaned alumni profile structure:")
            first_org = list(cleaned_alumni_data.keys())[0]
            if 'profiles' in cleaned_alumni_data[first_org] and cleaned_alumni_data[first_org]['profiles']:
                sample_profile = cleaned_alumni_data[first_org]['profiles'][0]
                print(json.dumps({k: v for k, v in sample_profile.items() if k in ['name', 'headline', 'about']}, indent=2))
        
        print(f"\nOutput files saved to:")
        if cleaned_student_data:
            print(f"- s3://{S3_BUCKET}/{cleaned_student_profiles_key}")
        if cleaned_alumni_data:
            print(f"- s3://{S3_BUCKET}/{cleaned_alumni_profiles_key}")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()