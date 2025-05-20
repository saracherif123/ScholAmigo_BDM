from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat, split, explode, array, regexp_replace, lower, trim

spark = SparkSession.builder.appName("AggregatorProcessor").getOrCreate()

# Read Aggregator data (JSON array, so use multiLine=True)
agg_df = spark.read.json("landing_zone/data/aggregator_data/scholarships_aggregate.json", multiLine=True)
print("Aggregator Data Schema:")
agg_df.printSchema()
print("Sample Aggregator Data:")
agg_df.show(2, truncate=False)

# Perform additional processing using the formatter logic
# For example, filtering, transformations, etc.
# This is a placeholder for the actual formatter logic

# Save the processed data as a cleaned JSON file
agg_df.write.json("/Users/sarasaad/Documents/BDMA /UPC/BDM/Project/BDM Scholamigo/ScholAmigo_BDM/trusted_zone/data/aggregator_data", mode="overwrite")

spark.stop() 