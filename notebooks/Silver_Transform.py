# Databricks notebook source
from pyspark.sql.functions import col, to_date, to_timestamp, upper, coalesce, lit

# --- 1. Define Table Names ---
bronze_table = "workspace.default.bug_list_bronze"
silver_table = "workspace.default.bug_list_silver"

print(f"Reading from: {bronze_table}")
print(f"Transforming and writing to: {silver_table}")

# --- 2. Extract from Bronze ---
try:
    df_bronze = spark.read.table(bronze_table)
except Exception as e:
    print(f"Error reading Bronze table '{bronze_table}': {e}")
    dbutils.notebook.exit("Failed to read Bronze table")

# --- 3. Transform (Using CORRECT Column Names) ---
# We are now using the column names from your error message.

try:
    df_silver = df_bronze.select(
        col("ID").alias("bug_id"),
        col("Summary"),
        col("Product"),
        col("Component"),
        
        # Standardize Status to uppercase (e.g., "Open" -> "OPEN")
        upper(col("Status")).alias("status"),
        
        # Map 'Severity' to 'priority' and standardize to uppercase
        upper(col("Severity")).alias("priority"),
        
        # Fill null "Assignee" values with "Unassigned"
        coalesce(col("Assignee"), lit("Unassigned")).alias("assignee"),
        
        # Convert the "Changed" string to a real date.
        # First, convert to timestamp, then cast to just the date.
        to_date(to_timestamp(col("Changed"), "MM/dd/yyyy HH:mm")).alias("changed_date")
    )
except Exception as e:
    print(f"Error during transformation. Your columns are: {df_bronze.columns}")
    print(e)
    dbutils.notebook.exit("Failed in transformation step")


# --- 4. Load to Silver Table ---
# This saves the cleaned data as a new Silver managed table
try:
    df_silver.write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable(silver_table)
except Exception as e:
    print(f"Error writing Silver table '{silver_table}': {e}")
    dbutils.notebook.exit("Failed to write Silver table")

print("---")
print(f"Silver table '{silver_table}' created successfully.")
print("You can now query this table using 'SELECT * FROM {silver_table}'")

display(df_silver)