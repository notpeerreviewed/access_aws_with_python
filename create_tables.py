import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import boto3
import create_redshift as cr
from db_connection import create_connection


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    myClusterProps = cr.redshift.describe_clusters(ClusterIdentifier=cr.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']

    # connect to database
    conn = create_connection(
        cr.DWH_DB, cr.DWH_DB_USER, cr.DWH_DB_PASSWORD, DWH_ENDPOINT, cr.DWH_PORT
    )

    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()