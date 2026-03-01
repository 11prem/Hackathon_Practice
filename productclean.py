import pandas as pd
import re
import logging

# ---------------------------------------------------
# 🔹 Logging Configuration
# ---------------------------------------------------
logging.basicConfig(
    filename="product_cleaning.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Product Cleaning Process Started")

# ---------------------------------------------------
# 🔹 Load Data
# ---------------------------------------------------
file_path = r"C:\Users\milky\OneDrive\Desktop\HCLMOCK\products.csv"
df = pd.read_csv(file_path)

# Normalize column names (VERY IMPORTANT)
df.columns = df.columns.str.strip().str.lower()

rows_before = len(df)
logging.info(f"Initial Rows: {rows_before}")
logging.info(f"Columns Found: {df.columns.tolist()}")

# ---------------------------------------------------
# 1️⃣ Remove Missing product_id (Critical)
# ---------------------------------------------------
if "product_id" in df.columns:
    missing_product_id = df["product_id"].isna().sum()
    df = df[df["product_id"].notna()]
    logging.info(f"Rows removed due to missing product_id: {missing_product_id}")
else:
    logging.warning("product_id column not found")

# ---------------------------------------------------
# 2️⃣ Remove Missing product_name (Critical)
# ---------------------------------------------------
if "product_name" in df.columns:
    missing_product_name = df["product_name"].isna().sum()
    df = df[df["product_name"].notna()]
    logging.info(f"Rows removed due to missing product_name: {missing_product_name}")
else:
    logging.warning("product_name column not found")

# ---------------------------------------------------
# 3️⃣ Clean Price
# ---------------------------------------------------
if "price" in df.columns:
    df["price"] = df["price"].astype(str).str.strip().str.lower()

    # Replace 'free'
    df.loc[df["price"] == "free", "price"] = "0"

    # Remove currency symbols
    df["price"] = df["price"].replace(r"[^\d\.]", "", regex=True)

    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    invalid_price = df["price"].isna().sum()
    logging.info(f"Invalid price entries after cleaning: {invalid_price}")

    # Remove negative & zero prices
    negative_prices = (df["price"] < 0).sum()
    zero_prices = (df["price"] == 0).sum()

    df = df[df["price"] > 0]

    logging.info(f"Negative prices removed: {negative_prices}")
    logging.info(f"Zero prices removed: {zero_prices}")
else:
    logging.warning("price column not found")

# ---------------------------------------------------
# 4️⃣ Clean Cost
# ---------------------------------------------------
if "cost" in df.columns:
    df["cost"] = df["cost"].astype(str).str.strip()
    df["cost"] = df["cost"].replace(r"[^\d\.]", "", regex=True)
    df["cost"] = pd.to_numeric(df["cost"], errors="coerce")

    invalid_cost = df["cost"].isna().sum()
    logging.info(f"Invalid cost entries: {invalid_cost}")

    # Remove cost > price
    if "price" in df.columns:
        cost_greater_price = (df["cost"] > df["price"]).sum()
        df = df[df["cost"] <= df["price"]]
        logging.info(f"Rows removed where cost > price: {cost_greater_price}")
else:
    logging.warning("cost column not found")

# ---------------------------------------------------
# 5️⃣ Standardize Category Naming
# ---------------------------------------------------
if "category" in df.columns:
    df["category"] = df["category"].astype(str).str.strip().str.lower()

    df["category"] = df["category"].replace({
        "electronics ": "electronics",
        "elec": "electronics",
        "electronic": "electronics"
    })

    logging.info("Category names standardized")
else:
    logging.warning("category column not found")

# ---------------------------------------------------
# 6️⃣ Fix Mixed Date Formats (Safe)
# ---------------------------------------------------
# Detect possible date columns dynamically
possible_date_cols = ["created_date", "launch_date", "added_date"]

for col in possible_date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(
            df[col],
            errors="coerce",
            dayfirst=True
        )
        invalid_dates = df[col].isna().sum()
        logging.info(f"Invalid {col} entries: {invalid_dates}")

# ---------------------------------------------------
# 7️⃣ Validate Discontinued Flag
# ---------------------------------------------------
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

    invalid_discontinued = (~df["discontinued"].isin([True, False])).sum()
    logging.info(f"Invalid discontinued flags: {invalid_discontinued}")

    df.loc[~df["discontinued"].isin([True, False]), "discontinued"] = None
else:
    logging.warning("discontinued column not found")

# ---------------------------------------------------
# 8️⃣ Remove Completely Malformed Rows
# (More than 50% null values)
# ---------------------------------------------------
threshold = int(len(df.columns) * 0.5)
malformed_rows = df.isnull().sum(axis=1) > threshold
removed_malformed = malformed_rows.sum()

df = df[~malformed_rows]

logging.info(f"Completely malformed rows removed: {removed_malformed}")

# ---------------------------------------------------
# 🔹 Final Summary
# ---------------------------------------------------
rows_after = len(df)
rows_removed = rows_before - rows_after

logging.info(f"Final Rows: {rows_after}")
logging.info(f"Total Rows Removed: {rows_removed}")

print("🔹 Product Cleaning Completed")
print("Rows Before:", rows_before)
print("Rows After:", rows_after)
print("Rows Removed:", rows_removed)

# Save cleaned dataset
df.to_csv("products_cleaned.csv", index=False)

logging.info("Product Cleaning Process Completed Successfully")