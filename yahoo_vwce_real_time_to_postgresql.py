import yfinance as yf
import pandas as pd
import schedule
import time
from datetime import datetime
import psycopg2
from psycopg2 import sql

DB_NAME = 'yahoo'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
DB_HOST = 'localhost'
DB_PORT = '5432'

#................................................................................................................
def create_database_if_not_exists():
    conn = psycopg2.connect(
                        dbname='postgres',
                        user=DB_USER,
                        password=DB_PASSWORD,
                        host=DB_HOST,
                        port=DB_PORT
                           )
    conn.autocommit = True #if a transaction is not committed, changes will not be saved
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}';") #checks if a database with the name specified by DB_NAME exists in the PostgreSQL server
        exists = cursor.fetchone() # fetches the result of the previously executed SQL query
        if not exists:
            cursor.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(DB_NAME)))
            print(f"Database {DB_NAME} created")
        else:
            print(f"Database {DB_NAME} already exists")
    conn.close()


def create_table_if_not_exists(conn):
    create_table_query = '''
                    CREATE TABLE IF NOT EXISTS vwce (
                                                        id SERIAL PRIMARY KEY,
                                                        datetime TIMESTAMP NOT NULL,
                                                        price NUMERIC NOT NULL
                                                    );
                         '''
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
        conn.commit()


#fetch the price and store it in the PostgreSQL table
def get_and_store_price(ticker, conn):
    etf = yf.Ticker(ticker)
    price = etf.history(period='1d')['Close'][0]
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    insert_query = sql.SQL('''
                            INSERT INTO vwce (datetime, price)
                            VALUES (%s, %s);
                            ''')
    with conn.cursor() as cursor:
        cursor.execute(insert_query, (current_time, price))
        conn.commit()
    print(f"Inserted data: {current_time}, {price}")
#............................................................................................................................

create_database_if_not_exists()

ticker = 'VWCE.DE'

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(
                        dbname=DB_NAME,
                        user=DB_USER,
                        password=DB_PASSWORD,
                        host=DB_HOST,
                        port=DB_PORT
                        )
                        
    create_table_if_not_exists(conn)
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit(1)

# Schedule the function to run every minute
schedule.every(1).minutes.do(get_and_store_price, ticker=ticker, conn=conn)


print("Starting to fetch and store ETF prices every minute...")

# Keep the script running
while True:
    schedule.run_pending() #checks if any scheduled tasks are due to run and executes them
    time.sleep(1) #pauses the loop for 1 second to prevent it from consuming too much CPU

