import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    
    Description: Copies data from s3 to staging tables.
    
    Arguments:
        cur: the cursor object
        conn: the connection object
        
    Returns:
        None
        
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: ETL from staging tables to facts and dimension tables.
    
    Arguments:
        cur: the cursor object
        conn: the connection object
        
    Returns:
        None
    
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    
    Description: 
        - establishes connection with database.
        - loads staging tables to redshift database.
        - performs ETL utilising SQL to insert from staging tables into facts and dimension tables.
        
    Returns:
        None
    
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()