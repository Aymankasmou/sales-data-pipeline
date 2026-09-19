# API to PostgreSQL ETL Pipeline

A simple ETL pipeline using Apache Airflow, Python, and PostgreSQL.

The pipeline gets sales data from the DummyJSON API, transforms the data, and stores it in PostgreSQL.

### Pipeline


API -> Extract -> Transform -> sales_raw -> sales_curated -> Validate

### Tools

* Python
* Apache Airflow
* PostgreSQL
* Pandas
* SQL

### Tables

**sales_raw**
Stores the data after loading it from the API.

**sales_curated**
Stores the cleaned data with duplicate records removed.

### Airflow Tasks

* extract_from_api
* transform_data
* load_to_raw
* create_curated
* validate_data
