import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all existing tables (7) in Sparkify.
    Two staging tables: staging_events, staging_songs
    Five tables: songplay, songs, artists, users and time.

    INPUTS:
    * cur the cursor variable
    * con the connection to the Postgres DB 
    """
    print("1. Droping existing tables:")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        query_words = query.split()
        print("- Table: `", query_words[-1], "`", sep='')


def create_tables(cur, conn):
    """
    Creates tables (7) in Sparkify.
    Two staging tables: staging_events, staging_songs
    Five tables: time, users, artists, songs and songplay.

    INPUTS:
    * cur the cursor variable
    * con the connection to the Postgres DB
    """
    print("2. Creating tables.")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        query_words = query.split()
        print("- Table: `", query_words[5], "`", sep='')


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
    This is the main function. It executes the create tables script.
    First, it retrieves the parameters from the dwh.cfg file.
    Then, it creates a connection and a cursor.
    Then, it drops the tables if they exist.
    Then, it closes the connection. 
    """
    config = configparser.ConfigParser()
    config.optionxform=str
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*getDBCredentials(config))
        )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()