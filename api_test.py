import requests
import pandas as pd
from sqlalchemy import create_engine

url = "https://dummyjson.com/carts"

response = requests.get(url)

data = response.json()

carts = data["carts"]

sales = []

for cart in carts:

    for product in cart["products"]:

        sale = {
            "cart_id": cart["id"],
            "product_id": product["id"],
            "title": product["title"],
            "price": product["price"],
            "quantity": product["quantity"],
            "total": product["total"]
        }

        sales.append(sale)


df = pd.DataFrame(sales)

# Save CSV
df.to_csv("sales.csv", index=False)


# PostgreSQL connection
engine = create_engine(
    "postgresql+psycopg2://postgres:1234@localhost:5452/postgres"
)

# Load data into PostgreSQL
df.to_sql(
    "sales_api",
    engine,
    if_exists="replace",
    index=False
)

print(df)
print("Number of rows:", len(df))
print("Data loaded into PostgreSQL successfully!")