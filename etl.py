import configparser
import psycopg2
import boto3
from sql_queries import copy_table_queries, insert_table_queries
from db_connection import create_connection
import create_redshift as cr

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    
    myClusterProps = cr.redshift.describe_clusters(ClusterIdentifier=cr.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']

    conn = create_connection(
        cr.DWH_DB, cr.DWH_DB_USER, cr.DWH_DB_PASSWORD, DWH_ENDPOINT, cr.DWH_PORT
    )

    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()