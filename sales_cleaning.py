import pandas as pd
import numpy as np
import logging

logging.basicConfig(
    filename="sales_cleaning.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Data Cleaning Started")

# -------------------- Load --------------------
df = pd.read_csv("sales.csv", dtype=str, on_bad_lines="skip")
logging.info(f"Initial rows: {len(df)}")

# -------------------- Remove Duplicates --------------------
df = df.drop_duplicates(subset=["sale_id"])
logging.info(f"After duplicate removal: {len(df)}")

# -------------------- Quantity --------------------
df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

# Replace missing/invalid quantity with median
median_qty = df["quantity"].median()
df["quantity"] = df["quantity"].fillna(median_qty)

# Replace zero/negative with median
df.loc[df["quantity"] <= 0, "quantity"] = median_qty

logging.info(f"After quantity fixing: {len(df)}")

# -------------------- Unit Price --------------------
df["unit_price"] = df["unit_price"].str.replace(r"[^\d.]", "", regex=True)
df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

median_price = df["unit_price"].median()
df["unit_price"] = df["unit_price"].fillna(median_price)
df.loc[df["unit_price"] <= 0, "unit_price"] = median_price

logging.info(f"After price fixing: {len(df)}")

# -------------------- Total Amount --------------------
df["total_amount"] = df["quantity"] * df["unit_price"]

# -------------------- Sale Date --------------------
df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce", dayfirst=True)

# Replace invalid dates with most common date
most_common_date = df["sale_date"].mode()[0]
df["sale_date"] = df["sale_date"].fillna(most_common_date)

logging.info(f"After date fixing: {len(df)}")

# -------------------- Payment Type --------------------
df["payment_type"] = df["payment_type"].str.strip().str.lower()
df["payment_type"] = df["payment_type"].fillna("unknown")

# -------------------- Discount --------------------
df["discount"] = df["discount"].str.replace("%", "", regex=False)
df["discount"] = pd.to_numeric(df["discount"], errors="coerce")
df["discount"] = df["discount"].fillna(0)
df.loc[df["discount"] < 0, "discount"] = 0
df["discount"] = df["discount"] / 100

# -------------------- Store ID --------------------
df["store_id"] = df["store_id"].str.strip()
df["store_id"] = df["store_id"].replace("", "UNKNOWN_STORE")

# -------------------- Foreign Key Handling (SAFE MODE) --------------------
customers = pd.read_csv("customers_cleaned.csv", dtype=str)
products = pd.read_csv("products_cleaned.csv", dtype=str)

df["customer_id"] = df["customer_id"].str.strip().str.upper()
df["product_id"] = df["product_id"].str.strip().str.upper()

customers["customer_id"] = customers["customer_id"].str.strip().str.upper()
products["product_id"] = products["product_id"].str.strip().str.upper()

# Add data quality flags instead of deleting
df["valid_customer"] = df["customer_id"].isin(customers["customer_id"])
df["valid_product"] = df["product_id"].isin(products["product_id"])

# Replace invalid ones with placeholder
df.loc[~df["valid_customer"], "customer_id"] = "UNKNOWN_CUSTOMER"
df.loc[~df["valid_product"], "product_id"] = "UNKNOWN_PRODUCT"

logging.info(f"After foreign key handling: {len(df)}")

# -------------------- Save --------------------
df.to_csv("sales_cleaned.csv", index=False)

logging.info("sales_cleaned.csv generated successfully")
logging.info(f"Final row count: {len(df)}")
logging.info("Data Cleaning Completed")