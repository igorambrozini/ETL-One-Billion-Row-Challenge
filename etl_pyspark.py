from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, min, max, avg
import random
import pandas as pd
import os

os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ETL One-Billion-Row-Challenge") \
    .getOrCreate()

# Load the cities list
cities = pd.read_parquet('data\\cities.parquet')
cities_list = list(cities.values.tolist())

# Defining table schema (city, temperature)
schema = StructType([
    StructField("City", StringType(), False),
    StructField("Temperature", FloatType(), True)
])

# Generate data to 1 billion rows (for testing, we use fewer rows)
num_rows = 100000

def generate_random_temp():
    return random.uniform(-99.0, 99.0)

# Create RDD (PySpark) with the data
data = spark.sparkContext.parallelize(range(num_rows)) \
    .map(lambda x: (cities_list[x % len(cities_list)][0], generate_random_temp()))

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show a sample of the DataFrame
df.show()

# Defining schema for the aggregated results
agg_schema = StructType([
    StructField("city", StringType(), False),
    StructField("min_value", FloatType(), True),
    StructField("max_value", FloatType(), True),
    StructField("avg_value", FloatType(), True)
])

# Calculate min, max, avg values (group by cities)
agg_df = df.groupBy("City") \
    .agg(min(col("Temperature")).alias("min_value"),
         max(col("Temperature")).alias("max_value"),
         avg(col("Temperature")).alias("avg_value"))

# Show aggregated results
agg_df.show()
