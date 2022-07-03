import psycopg2
from psycopg2 import OperationalError

def create_connection(DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT):
    connection = None
    try:
        connection = psycopg2.connect(
            database=DWH_DB,
            user=DWH_DB_USER,
            password=DWH_DB_PASSWORD,
            host=DWH_ENDPOINT,
            port=DWH_PORT,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

