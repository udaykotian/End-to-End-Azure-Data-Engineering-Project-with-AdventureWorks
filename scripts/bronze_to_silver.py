# Databricks notebook for Bronze to Silver transformation

# Set up Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# Define paths
bronze_path = "abfss://bronze@adlsmrktalkstech.dfs.core.windows.net/SalesLT/"
silver_path = "abfss://silver@adlsmrktalkstech.dfs.core.windows.net/SalesLT/"

# List tables in Bronze layer
tables = [f.name.split('/')[0] for f in dbutils.fs.ls(bronze_path)]

# Transform each table
for table in tables:
    print(f"Transforming {table} from Bronze to Silver...")
    # Read from Bronze
    df = spark.read.format("delta").load(f"{bronze_path}{table}/")
    # Perform cleaning (e.g., date formatting)
    if 'OrderDate' in df.columns:
        df = df.withColumn('OrderDate', df['OrderDate'].cast('date'))
    # Remove nulls
    df = df.na.drop()
    # Write to Silver
    df.write.format("delta").mode("overwrite").save(f"{silver_path}{table}/")
    print(f"Transformed {table} saved to Silver layer.")
