import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
config = configparser.ConfigParser()
config.read_file(open(r'Data_Warehouse_Project_Template\dwh.cfg'))

#Credentials
host = config.get('CLUSTER','HOST')
dbname = config.get('CLUSTER','DB_NAME')
user = config.get('CLUSTER','DB_USER')
password = config.get('CLUSTER','DB_PASSWORD')
port = config.get('CLUSTER','DB_PORT')

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, dbname, user, password, port))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()