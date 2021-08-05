import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 to Sparkify staging tables.
    Target staging tables: staging_events, staging_songs

    INPUTS:
    * cur the cursor variable
    * con the connection to the Postgres DB 
    """
    print("1. Loading data from S3 to Redshift staging tables.")
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        query_words = query.split()
        print("- Table loaded: `", query_words[1], "`", sep='')


def insert_tables(cur, conn):
    """
    Loads data from staging tables to final tables.
    Target staging tables: songplay, songs, artists, users and time

    INPUTS:
    * cur the cursor variable
    * con the connection to the Postgres DB 
    """
    print("2. Transforming from staging to final.")
    for query in insert_table_queries[:3]:
        cur.execute(query)
        conn.commit()
        query_words = query.split()
        print("- Table loaded: `", query_words[2], "`", sep='')

def getDBCredentials(config):
    """
    Retrieves a list with the required credentials to connect to the DB

    INPUT:
    * config ConfigParser() object with parameters
    """
    credentials = []
    credentials.append(config['DWH']['DWH_ENDPOINT'])
    credentials.append(config['DWH']['DWH_DB'])
    credentials.append(config['DWH']['DWH_DB_USER'])
    credentials.append(config['DWH']['DWH_DB_PASSWORD'])
    credentials.append(config['DWH']['DWH_PORT'])
    return credentials

def main():
    """
    This is the main function. It executes the etl script.
    First, it retrieves the parameters from the dwh.cfg file.
    Then, it creates a connection and a cursor.
    Then, it loads the data from S3 to staging tables.
    Then, it processes the data from staging tables to final tables in Redshift
    Then, it closes the connection. 
    """
    config = configparser.ConfigParser()
    config.optionxform=str
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*getDBCredentials(config))
        )
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()