import pandas as pd
import sqlite3

conn = sqlite3.connect("retail.db")

customers = pd.read_csv("customers_cleaned.csv")
products = pd.read_csv("products_cleaned.csv")
sales = pd.read_csv("sales_cleaned.csv")

customers.to_sql("customers", conn, if_exists="replace", index=False)
products.to_sql("products", conn, if_exists="replace", index=False)
sales.to_sql("sales", conn, if_exists="replace", index=False)

conn.commit()
conn.close()

print("All datasets successfully loaded into retail.db")

sales["product_id"] = pd.to_numeric(sales["product_id"], errors="coerce")
sales = sales.dropna(subset=["product_id"])
sales["product_id"] = sales["product_id"].astype(int)
# Merge sales with products to get price
sales_merged = sales.merge(products, on="product_id", how="left")

# Revenue per transaction
sales_merged["total_revenue"] = sales_merged["quantity"] * sales_merged["price"]
# Calculate total revenue
total_revenue = sales_merged["total_revenue"].sum()
print("Total Revenue:", total_revenue)

top_products = (
    sales_merged
    .groupby("product_name")["total_revenue"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

print(top_products)

worst_products = (
    sales_merged
    .groupby("product_name")["total_revenue"]
    .sum()
    .sort_values()
    .head(10)
)

print(worst_products)

sales_with_customers = pd.concat([sales_merged, customers], axis=0, join="outer", ignore_index=True)

top_customers = (
    sales_with_customers
    .groupby("customer_id")["total_revenue"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

print(top_customers)

sales_merged["sale_month"] = pd.to_datetime(sales_merged["sale_date"]).dt.to_period("M")

monthly_trend = (
    sales_merged
    .groupby("sale_month")["total_revenue"]
    .sum()
    .reset_index()
)

print(monthly_trend)

category_performance = (
    sales_merged
    .groupby("category")["total_revenue"]
    .sum()
    .sort_values(ascending=False)
)

print(category_performance)

customer_orders = sales.groupby("customer_id").size()

repeat_customers = customer_orders[customer_orders > 1].count()
total_customers = customer_orders.count()

repeat_percentage = (repeat_customers / total_customers) * 100

print("Repeat Customer %:", repeat_percentage)