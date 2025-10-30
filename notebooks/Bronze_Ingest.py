# Databricks notebook source
# --- 1. Define Paths ---
# This is the path to the CSV you uploaded. (No 'dbfs:' needed)
raw_data_path = "/Volumes/workspace/default/gcs_keys/Bug List (1).csv"

# We will save the table directly, so we don't need a write path variable.
print(f"Reading from: {raw_data_path}")
print(f"Saving table to: workspace.default.bug_list_bronze")

# --- 2. Extract & Load (EL) ---
# Read the raw CSV file from the Volume
try:
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(raw_data_path)
except Exception as e:
    print(f"Error reading CSV from Volume. Check your file path.")
    print(e)
    dbutils.notebook.exit("Failed to read raw data")

# --- 3. Write to Bronze Managed Table ---
# This is the new, correct way.
# It saves the data AND creates the table in Unity Catalog.
try:
    df.write.format("delta") \
      .mode("overwrite") \
      .saveAsTable("workspace.default.bug_list_bronze") # Use 3-level name
except Exception as e:
    print(f"Error writing table 'workspace.default.bug_list_bronze': {e}")
    dbutils.notebook.exit("Failed to write Bronze table")

# --- 4. Success ---
print("---")
print("Bronze table 'workspace.default.bug_list_bronze' created successfully.")
print("You can now query this table using 'SELECT * FROM workspace.default.bug_list_bronze'")

display(df)