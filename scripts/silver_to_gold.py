# Databricks notebook for Silver to Gold transformation

# Set up Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

# Define paths
silver_path = "abfss://silver@adlsmrktalkstech.dfs.core.windows.net/SalesLT/"
gold_path = "abfss://gold@adlsmrktalkstech.dfs.core.windows.net/SalesLT/"

# List tables in Silver layer
tables = [f.name.split('/')[0] for f in dbutils.fs.ls(silver_path)]

# Transform each table
for table in tables:
    print(f"Transforming {table} from Silver to Gold...")
    # Read from Silver
    df = spark.read.format("delta").load(f"{silver_path}{table}/")
    # Rename columns (e.g., SalesOrderID to Sales_Order_ID)
    for col in df.columns:
        new_col = col.replace('ID', '_ID')
        df = df.withColumnRenamed(col, new_col)
    # Write to Gold
    df.write.format("delta").mode("overwrite").save(f"{gold_path}{table}/")
    print(f"Transformed {table} saved to Gold layer.")
