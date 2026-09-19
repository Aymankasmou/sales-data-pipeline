from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

import requests
import pandas as pd


POSTGRES_CONN_ID = "postgres_sales"
API_URL = "https://dummyjson.com/carts"


@dag(
    dag_id="api_to_postgres",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "api", "postgres"],
    default_args={
        "retries": 2,
    },
)
def api_to_postgres():

    @task
    def extract_from_api():

        response = requests.get(
            API_URL,
            timeout=30
        )

        response.raise_for_status()

        data = response.json()

        carts = data["carts"]

        print(f"Number of carts received: {len(carts)}")

        return carts

    @task
    def transform_data(carts):

        sales = []

        for cart in carts:

            for product in cart["products"]:

                sales.append({
                    "cart_id": cart["id"],
                    "product_id": product["id"],
                    "title": product["title"],
                    "price": product["price"],
                    "quantity": product["quantity"],
                    "total": product["total"],
                })

        df = pd.DataFrame(sales)

        print(f"Number of rows after transformation: {len(df)}")

        return df.to_dict(orient="records")

    @task
    def load_to_raw(sales_data):

        df = pd.DataFrame(sales_data)

        postgres_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        engine = postgres_hook.get_sqlalchemy_engine()

        df.to_sql(
            "sales_raw",
            engine,
            if_exists="append",
            index=False,
        )

        print(f"Loaded {len(df)} rows into sales_raw")

    @task
    def create_curated():

        postgres_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        sql = """
        TRUNCATE TABLE sales_curated;

        INSERT INTO sales_curated (
            cart_id,
            product_id,
            title,
            price,
            quantity,
            total
        )
        SELECT DISTINCT
            cart_id,
            product_id,
            title,
            price,
            quantity,
            total
        FROM sales_raw;
        """

        postgres_hook.run(sql)

        print("Curated table updated successfully")

    @task
    def validate_data():

        postgres_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        result = postgres_hook.get_first(
            """
            SELECT
                (SELECT COUNT(*) FROM sales_raw),
                (SELECT COUNT(*) FROM sales_curated);
            """
        )

        raw_count = result[0]
        curated_count = result[1]

        print(f"Raw rows: {raw_count}")
        print(f"Curated rows: {curated_count}")

        if curated_count == 0:
            raise ValueError(
                "Validation failed: sales_curated is empty"
            )

        if curated_count > raw_count:
            raise ValueError(
                "Validation failed: curated rows exceed raw rows"
            )

        print("Data validation successful!")

    carts = extract_from_api()

    sales_data = transform_data(carts)

    raw_task = load_to_raw(sales_data)

    curated_task = create_curated()

    validation_task = validate_data()

    raw_task >> curated_task >> validation_task


api_to_postgres()