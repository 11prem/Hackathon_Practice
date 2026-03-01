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