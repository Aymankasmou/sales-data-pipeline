import psycopg2
from getpass import getpass

password = getpass("Enter PostgreSQL password: ")

conn = psycopg2.connect(
    host="localhost",
    port=5452,
    database="postgres",
    user="postgres",
    password=password
)

print("PostgreSQL connection successful!")

conn.close()