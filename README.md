# Sales Data Pipeline

An end-to-end data engineering pipeline that extracts sales data from a REST API, transforms nested JSON data, loads it into PostgreSQL, removes duplicates, and validates the data using Apache Airflow.

---

## Project Overview

This project simulates a real-world ETL pipeline where data is collected from an external API, transformed into a structured format, stored in PostgreSQL, and validated automatically.

The pipeline is orchestrated using Apache Airflow.

---

## Architecture

```text
DummyJSON API -> Extract -> Transform -> sales_raw -> Deduplication -> sales_curated -> Validation
```

---

## Technologies

- Python
- Apache Airflow
- PostgreSQL
- Pandas
- SQL
- REST API
- Docker
- Astro CLI

---

## Data Source

API:

```text
https://dummyjson.com/carts
```

The API provides:

- Cart ID
- Product ID
- Product Title
- Price
- Quantity
- Total

---

## Pipeline Workflow

### 1. Extract

Retrieve cart data from the API with:

- Timeout handling
- HTTP error handling
- Airflow retries

Example:

```python
response = requests.get(API_URL, timeout=30)
response.raise_for_status()
```

---

### 2. Transform

Convert nested JSON into a tabular format:

```text
cart_id
product_id
title
price
quantity
total
```

Pandas is used for transformation.

---

### 3. Raw Layer

Store raw transformed data in:

```text
sales_raw
```

Additional metadata:

```text
ingested_at
```

Records the load timestamp.

---

### 4. Curated Layer

Create a cleaned table:

```text
sales_curated
```

Duplicates are removed using:

```sql
SELECT DISTINCT
```

---

### 5. Validation

Airflow validates:

- Raw row count
- Curated row count
- Non-empty curated table
- Curated rows do not exceed raw rows

---

## Airflow DAG

Main DAG:

```text
api_to_postgres
```

Workflow:

```text
extract_from_api -> transform_data -> load_to_raw -> create_curated -> validate_data
```

---

## Database Schema

### sales_raw

| Column | Type |
|----------|----------|
| cart_id | INTEGER |
| product_id | INTEGER |
| title | VARCHAR |
| price | NUMERIC |
| quantity | INTEGER |
| total | NUMERIC |
| ingested_at | TIMESTAMPTZ |

### sales_curated

| Column | Type |
|----------|----------|
| cart_id | INTEGER |
| product_id | INTEGER |
| title | VARCHAR |
| price | NUMERIC |
| quantity | INTEGER |
| total | NUMERIC |

---

## Example Analysis

Top products by revenue:

```sql
SELECT
    product_id,
    title,
    SUM(quantity) AS total_quantity,
    ROUND(SUM(total), 2) AS total_revenue
FROM sales_curated
GROUP BY product_id, title
ORDER BY total_revenue DESC;
```

Example output:

```text
Durango SXT RWD            184999.95
Dodge Hornet GT Plus       124999.95
Rolex Datejust Women       109999.90
Rolex Datejust              76999.93
MotoGP CI.H1                59999.96
```

---

## Project Structure

```text
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
```

---

## Running the Project

### Clone Repository

```bash
git clone https://github.com/Aymankasmou/sales-data-pipeline.git
cd sales-data-pipeline
```

### Start Airflow

```bash
astro dev start
```

### Airflow Connection

```text
Connection ID: postgres_sales
Type: Postgres
Host: host.docker.internal
Port: 5452
Database: postgres
```

Credentials are stored in Airflow Connections and are not included in GitHub.

### Run DAG

Trigger:

```text
api_to_postgres
```

---

## Data Quality Features

- API error handling
- Request timeout
- Retries
- Raw data preservation
- Deduplication
- Validation checks
- Raw / Curated separation

---

## Future Improvements

- Incremental loading
- Better deduplication strategy
- Additional data quality checks
- Scheduling
- Analytics layer
- Dashboard integration
- Monitoring

---

## Author

**Ayman Kasmou**

Bachelor's in Statistics and Computer Science

Interested in:

- Data Engineering
- Data Analytics
- Databases
- Python
- SQL
- Cloud Data Platforms
