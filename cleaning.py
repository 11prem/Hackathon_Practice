import pandas as pd
import re
import logging

# ---------------------------------------------------
# 🔹 Logging Configuration
# ---------------------------------------------------
logging.basicConfig(
    filename="customer_cleaning.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Customer Cleaning Process Started")

# ---------------------------------------------------
# 🔹 Load Data
# ---------------------------------------------------
file_path = "customers.csv"   # change path if needed
df = pd.read_csv(r"C:\Users\milky\OneDrive\Desktop\HCLMOCK\customers.csv")

rows_before = len(df)
logging.info(f"Initial Rows: {rows_before}")

# ---------------------------------------------------
# 1️⃣ Remove Duplicate customer_id
# ---------------------------------------------------
duplicate_count = df.duplicated(subset="customer_id").sum()
df = df.drop_duplicates(subset="customer_id", keep="first")
logging.info(f"Duplicate customer_id removed: {duplicate_count}")

# ---------------------------------------------------
# 2️⃣ Remove Missing customer_id (Critical Field)
# ---------------------------------------------------
missing_id_count = df["customer_id"].isna().sum()
df = df[df["customer_id"].notna()]
logging.info(f"Rows removed due to missing customer_id: {missing_id_count}")

# ---------------------------------------------------
# 3️⃣ Email Cleaning & Validation
# ---------------------------------------------------
df["email"] = df["email"].astype(str).str.strip()

def validate_email(email):
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return bool(re.match(pattern, email))

df["email_valid"] = df["email"].apply(validate_email)

invalid_email_count = (~df["email_valid"]).sum()
logging.info(f"Invalid email count: {invalid_email_count}")

# Nullify invalid emails (do not drop row)
df.loc[~df["email_valid"], "email"] = None
df.drop(columns=["email_valid"], inplace=True)

# ---------------------------------------------------
# 4️⃣ Phone Cleaning
# ---------------------------------------------------
def clean_phone(phone):
    phone = re.sub(r"\D", "", str(phone))  # remove non-digits

    # Remove India country code if present
    if phone.startswith("91") and len(phone) == 12:
        phone = phone[2:]

    if len(phone) == 10:
        return phone
    else:
        return None

df["phone"] = df["phone"].apply(clean_phone)

invalid_phone_count = df["phone"].isna().sum()
logging.info(f"Invalid phone count: {invalid_phone_count}")

# ---------------------------------------------------
# 5️⃣ Fix Mixed Date Formats (Corrected with dayfirst=True)
# ---------------------------------------------------
df["join_date"] = pd.to_datetime(
    df["join_date"],
    errors="coerce",
    dayfirst=True
)

df["birthdate"] = pd.to_datetime(
    df["birthdate"],
    errors="coerce",
    dayfirst=True
)

invalid_join_date_count = df["join_date"].isna().sum()
invalid_birthdate_count = df["birthdate"].isna().sum()

logging.info(f"Invalid join_date count: {invalid_join_date_count}")
logging.info(f"Invalid birthdate count: {invalid_birthdate_count}")

# Optional: Remove future birthdates (extra validation)
df.loc[df["birthdate"] > pd.Timestamp.today(), "birthdate"] = None

# ---------------------------------------------------
# 6️⃣ Remove Trailing Spaces in City
# ---------------------------------------------------
df["city"] = df["city"].astype(str).str.strip()
logging.info("City names trimmed")

# ---------------------------------------------------
# 7️⃣ Postal Code Validation (Numeric Only)
# ---------------------------------------------------
df["postal_code"] = df["postal_code"].astype(str).str.strip()

invalid_postal_count = (~df["postal_code"].str.isnumeric()).sum()
logging.info(f"Invalid postal codes: {invalid_postal_count}")

# Nullify invalid postal codes
df.loc[~df["postal_code"].str.isnumeric(), "postal_code"] = None

# ---------------------------------------------------
# 8️⃣ Gender Standardization
# ---------------------------------------------------
df["gender"] = df["gender"].astype(str).str.strip().str.lower()

df["gender"] = df["gender"].replace({
    "male": "M",
    "m": "M",
    "female": "F",
    "f": "F"
})

invalid_gender_count = (~df["gender"].isin(["M", "F"])).sum()
logging.info(f"Invalid gender entries: {invalid_gender_count}")

# Nullify invalid gender
df.loc[~df["gender"].isin(["M", "F"]), "gender"] = None

# ---------------------------------------------------
# 🔹 Final Summary
# ---------------------------------------------------
rows_after = len(df)
rows_removed = rows_before - rows_after

logging.info(f"Final Rows: {rows_after}")
logging.info(f"Total Rows Removed: {rows_removed}")

print("🔹 Cleaning Completed")
print("Rows Before:", rows_before)
print("Rows After:", rows_after)
print("Rows Removed (Critical Only):", rows_removed)

# Save cleaned file
df.to_csv("customers_cleaned.csv", index=False)

logging.info("Customer Cleaning Process Completed Successfully")