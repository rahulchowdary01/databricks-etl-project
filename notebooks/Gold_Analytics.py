# Databricks notebook source
from pyspark.sql.functions import col, count

# --- 1. Define Table Names ---
silver_table = "workspace.default.bug_list_silver"
gold_table = "workspace.default.bug_list_gold_analytics"

print(f"Reading from: {silver_table}")
print(f"Creating analytics and writing to: {gold_table}")

# --- 2. Extract from Silver ---
try:
    df_silver = spark.read.table(silver_table)
except Exception as e:
    print(f"Error reading Silver table '{silver_table}': {e}")
    dbutils.notebook.exit("Failed to read Silver table")

# --- 3. Transform (Aggregate for Analytics) ---
# This is our "meaningful business insight".
# We create a new table that counts all bugs, grouped by their 'product' and 'status'.
try:
    df_gold_analytics = df_silver.groupBy("product", "status") \
        .agg(
            count("bug_id").alias("bug_count")
        ) \
        .orderBy(col("bug_count").desc()) # Sort to see the biggest problems first

except Exception as e:
    print(f"Error during aggregation. Your silver columns are: {df_silver.columns}")
    print(e)
    dbutils.notebook.exit("Failed in aggregation step")


# --- 4. Load to Gold Table ---
# This saves the aggregated data as our final Gold table
try:
    df_gold_analytics.write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable(gold_table)
except Exception as e:
    print(f"Error writing Gold table '{gold_table}': {e}")
    dbutils.notebook.exit("Failed to write Gold table")

print("---")
print(f"Gold analytics table '{gold_table}' created successfully.")
print(f"You can query this table: 'SELECT * FROM {gold_table}'")

display(df_gold_analytics)