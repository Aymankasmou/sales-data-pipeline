# Sales Data Pipeline

An end-to-end data engineering pipeline that extracts sales data from a REST API, transforms the nested data, loads it into PostgreSQL, handles duplicate records, and performs data validation using Apache Airflow.

## Project Overview

This project simulates a real-world ETL pipeline where data is collected from an external API, transformed into a structured format, stored in PostgreSQL, and validated automatically.

The pipeline uses Apache Airflow to orchestrate the workflow.

## Architecture


DummyJSON API -> Extract -> Transform -> sales_raw -> Deduplication -> sales_curated -> Validation

##Technologies
Python
Apache Airflow
PostgreSQL
Pandas
SQL
REST API
Docker
Astro CLI
Data Source

The project uses the DummyJSON Carts API:

https://dummyjson.com/carts

The API provides cart and product information such as:

Cart ID
Product ID
Product title
Price
Quantity
Total

The API is used for learning and pipeline development purposes.

Pipeline Workflow
1. Extract

Airflow sends a request to the REST API and retrieves the cart data.

The pipeline includes:

API timeout
HTTP error handling
Airflow retries
response = requests.get(
    API_URL,
    timeout=30
)

response.raise_for_status()
2. Transform

The API returns products nested inside carts.

The pipeline flattens the nested structure into tabular records:

cart_id
product_id
title
price
quantity
total

Pandas is used to transform the data before loading it into PostgreSQL.

3. Raw Layer

The original transformed records are stored in:

sales_raw

The table also contains:

ingested_at

which records when the data was loaded into PostgreSQL.

4. Curated Layer

The pipeline creates a cleaned dataset in:

sales_curated

Duplicate records are removed using SELECT DISTINCT.

This separates the raw ingestion layer from the cleaned/curated layer.

5. Data Validation

After loading and transformation, Airflow validates the data.

The pipeline checks:

Raw row count
Curated row count
Whether the curated table contains data
Whether the curated row count exceeds the raw row count

If validation fails, the Airflow task fails.

Airflow DAG

The main DAG is:

api_to_postgres

The workflow contains four main tasks:

extract_from_api
        |
        v
transform_data
        |
        v
load_to_raw
        |
        v
create_curated
        |
        v
validate_data
Database Structure
sales_raw

Stores the transformed API data before deduplication.

Column	Type
cart_id	INTEGER
product_id	INTEGER
title	VARCHAR
price	NUMERIC
quantity	INTEGER
total	NUMERIC
ingested_at	TIMESTAMPTZ
sales_curated

Stores the cleaned dataset after duplicate removal.

Column	Type
cart_id	INTEGER
product_id	INTEGER
title	VARCHAR
price	NUMERIC
quantity	INTEGER
total	NUMERIC
Data Quality

The pipeline includes basic data quality practices:

API error handling
Request timeout
Automatic retries
Raw data preservation
Duplicate removal
Row count validation
Separation between raw and curated data
Example Analysis

The curated data can be used to calculate product-level revenue:

SELECT
    product_id,
    title,
    SUM(quantity) AS total_quantity,
    ROUND(SUM(total), 2) AS total_revenue
FROM sales_curated
GROUP BY product_id, title
ORDER BY total_revenue DESC;

Example output:

Product                  Quantity    Revenue
------------------------------------------------
Durango SXT RWD              5      184999.95
Dodge Hornet GT Plus         5      124999.95
Rolex Datejust Women        10      109999.90
Rolex Datejust               7       76999.93
MotoGP CI.H1                 4       59999.96
Project Structure
sales-data-pipeline/
│
├── dags/
│   └── api_to_postgres.py
│
├── sql/
│   └── analytics.sql
│
├── README.md
│
├── requirements.txt
│
└── .gitignore
How to Run
1. Clone the repository
git clone https://github.com/Aymankasmou/sales-data-pipeline.git
cd sales-data-pipeline
2. Install dependencies

The project uses Apache Airflow and the PostgreSQL provider.

apache-airflow-providers-postgres
3. Start Airflow

If using Astro CLI:

astro dev start
4. Configure PostgreSQL Connection

Create an Airflow connection with:

Connection ID: postgres_sales
Connection Type: Postgres
Host: host.docker.internal
Port: 5452
Database: postgres

The username and password should be configured through the Airflow connection and should not be stored in the GitHub repository.

5. Run the DAG

Open the Airflow UI and trigger:

api_to_postgres

The DAG will extract the API data, transform it, load it into PostgreSQL, create the curated dataset, and validate the results.

Future Improvements

Planned improvements include:

Incremental data loading
Better duplicate handling using source record identifiers
More data quality checks
Scheduled pipeline execution
Analytics layer
Logging and monitoring
Dashboard integration
Improved API ingestion strategy
Author

Ayman Kasmou

Bachelor's in Statistics and Computer Science

Interested in:

Data Engineering
Data Analytics
Databases
Python
SQL
Cloud Data Platforms
