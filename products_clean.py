import pandas as pd
import numpy as np
import sqlite3
import logging
import re
import os

# -----------------------------
# Logging Configuration
# -----------------------------
logging.basicConfig(
    filename="products_cleaning.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("=== PRODUCTS CLEANING STARTED ===")

# -----------------------------
# Load Dataset
# -----------------------------
df = pd.read_csv("products.csv") 

if not os.path.exists("products.csv"):
    logging.error("Dataset file not found.")
    raise FileNotFoundError("Dataset file missing")

logging.info(f"Initial Shape: {df.shape}")
logging.debug(f"Columns Found: {df.columns.tolist()}")

# Standardize column names
df.columns = df.columns.str.strip().str.lower()

rows_before = len(df)

# -----------------------------
# Remove completely malformed rows
# -----------------------------
threshold = int(len(df.columns) * 0.5)
malformed_rows = df.isnull().sum(axis=1) > threshold
logging.info(f"Malformed rows removed: {malformed_rows.sum()}")
df = df[~malformed_rows]

# -----------------------------
# product_id validation
# -----------------------------
if "product_id" in df.columns:
    missing_pid = df["product_id"].isna().sum()
    logging.info(f"Missing product_id: {missing_pid}")
    df = df[df["product_id"].notna()]
else:
    logging.error("product_id column missing")

# Remove duplicates
dup_pid = df.duplicated(subset=["product_id"]).sum()
logging.info(f"Duplicate product_id removed: {dup_pid}")
df = df.drop_duplicates(subset=["product_id"])

# -----------------------------
# product_name validation
# -----------------------------
if "product_name" in df.columns:
    missing_name = df["product_name"].isna().sum()
    logging.info(f"Missing product_name: {missing_name}")
    df = df[df["product_name"].notna()]

# -----------------------------
# Price Cleaning
# -----------------------------
if "price" in df.columns:
    df["price"] = df["price"].astype(str).str.strip().str.lower()
    df.loc[df["price"] == "free", "price"] = "0"

    # Remove currency symbols
    df["price"] = df["price"].replace(r"[^\d\.]", "", regex=True)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    invalid_price = df["price"].isna().sum()
    logging.warning(f"Invalid price entries: {invalid_price}")

    # Remove negative & zero
    neg_price = (df["price"] < 0).sum()
    zero_price = (df["price"] == 0).sum()

    df = df[df["price"] > 0]

    logging.info(f"Negative prices removed: {neg_price}")
    logging.info(f"Zero prices removed: {zero_price}")

# -----------------------------
# Cost Cleaning
# -----------------------------
if "cost" in df.columns:
    df["cost"] = df["cost"].astype(str).str.strip()
    df["cost"] = df["cost"].replace(r"[^\d\.]", "", regex=True)
    df["cost"] = pd.to_numeric(df["cost"], errors="coerce")

    invalid_cost = df["cost"].isna().sum()
    logging.warning(f"Invalid cost entries: {invalid_cost}")

    if "price" in df.columns:
        cost_gt_price = (df["cost"] > df["price"]).sum()
        df = df[df["cost"] <= df["price"]]
        logging.info(f"Rows removed where cost > price: {cost_gt_price}")

# -----------------------------
# Category Normalization
# -----------------------------
if "category" in df.columns:
    df["category"] = df["category"].astype(str).str.strip().str.lower()

    df["category"] = df["category"].replace({
        "elec": "electronics",
        "electronic": "electronics",
        "electronics ": "electronics"
    })

    logging.info("Category standardized")

# -----------------------------
# Date Standardization
# -----------------------------
for col in ["created_date", "launch_date", "added_date"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
        logging.warning(f"Invalid {col}: {df[col].isna().sum()}")

# -----------------------------
# Discontinued Flag
# -----------------------------
if "discontinued" in df.columns:
    df["discontinued"] = df["discontinued"].astype(str).str.strip().str.lower()

    df["discontinued"] = df["discontinued"].replace({
        "yes": True,
        "no": False,
        "1": True,
        "0": False,
        "true": True,
        "false": False
    })

    invalid_flag = (~df["discontinued"].isin([True, False])).sum()
    logging.warning(f"Invalid discontinued flags: {invalid_flag}")

    df.loc[~df["discontinued"].isin([True, False]), "discontinued"] = None

# -----------------------------
# Final Reporting
# -----------------------------
rows_after = len(df)
logging.info(f"Final Shape: {df.shape}")
logging.info(f"Total rows removed: {rows_before - rows_after}")

print("Cleaning Completed")
print("Rows Before:", rows_before)
print("Rows After:", rows_after)

# -----------------------------
# Save Cleaned Data
# -----------------------------
df.to_csv("products_cleaned.csv", index=False)

con = sqlite3.connect("all.db")
df.to_sql("products_cleaned", con=con, if_exists="replace", index=False)
con.close()

logging.info("=== PRODUCTS CLEANING COMPLETED SUCCESSFULLY ===")