import os
import json
import re
from typing import Dict, Any, List
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, when, trim, udf
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, FloatType, ArrayType

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
    """Initialize Spark session with appropriate configurations for JDK 11"""
    spark = SparkSession.builder \
        .appName("StudentDataPreprocessing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    return spark

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

def process_student_profiles(file_path, spark):
    """Process student profiles data"""
    print(f"Processing student profiles from: {file_path}")
    
    # Read the JSONL file
    with open(file_path, 'r', encoding='utf-8') as f:
        data = [json.loads(line.strip()) for line in f if line.strip()]
    
    if not data:
        print("No data found in student profiles file")
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

def process_alumni_profiles(file_path, spark):
    """Process alumni LinkedIn profiles data"""
    print(f"Processing alumni profiles from: {file_path}")
    
    # Read the JSON file
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Create UDFs for cleaning functions
    clean_text_udf = udf(clean_text, StringType())
    
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

def save_cleaned_data(data, output_path, file_format='json'):
    """Save cleaned data to file"""
    print(f"Saving cleaned data to: {output_path}")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        if file_format == 'jsonl':
            for item in data:
                json.dump(item, f, ensure_ascii=False)
                f.write('\n')
        else:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"Data saved successfully to {output_path}")

def main():
    # Initialize Spark
    spark = initialize_spark()
    
    try:
        # File paths (update these to your actual file paths)
        student_profiles_path = "../../landing_zone/data/users_data/student_profiles.jsonl"
        alumni_profiles_path = "../../landing_zone/data/erasmus_linkedin_profiles/Erasmus_Linkedin_Profiles.json"
        
        # Output file paths
        cleaned_student_profiles_path = "../data/graph_data/cleaned_student_profiles.jsonl"
        cleaned_alumni_profiles_path = "../data/graph_data/cleaned_alumni_profiles.json"
        
        # Check if input files exist
        if not os.path.exists(student_profiles_path):
            print(f"Warning: {student_profiles_path} not found. Skipping student profiles processing.")
            cleaned_student_data = []
        else:
            # Process student profiles
            cleaned_student_data = process_student_profiles(student_profiles_path, spark)
            print(f"Processed {len(cleaned_student_data)} student profiles")
        
        if not os.path.exists(alumni_profiles_path):
            print(f"Warning: {alumni_profiles_path} not found. Skipping alumni profiles processing.")
            cleaned_alumni_data = {}
        else:
            # Process alumni profiles  
            cleaned_alumni_data = process_alumni_profiles(alumni_profiles_path, spark)
            total_alumni = sum(len(org_data.get('profiles', [])) for org_data in cleaned_alumni_data.values())
            print(f"Processed {total_alumni} alumni profiles")
        
        # Save cleaned data
        if cleaned_student_data:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(cleaned_student_profiles_path), exist_ok=True)
            save_cleaned_data(cleaned_student_data, cleaned_student_profiles_path, 'jsonl')
        
        if cleaned_alumni_data:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(cleaned_alumni_profiles_path), exist_ok=True)
            save_cleaned_data(cleaned_alumni_data, cleaned_alumni_profiles_path, 'json')
        
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
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()