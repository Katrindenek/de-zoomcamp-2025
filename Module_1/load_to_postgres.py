import pandas as pd
import psycopg2
import gzip
import io
import sys
import os
from urllib.request import urlopen
from sqlalchemy import create_engine

def load_csv_to_postgres(url, dbname, user, password, host, port, table_name):
    # Download the file
    response = urlopen(url)
    if url.endswith('.gz'):
        with gzip.GzipFile(fileobj=response) as f:
            df = pd.read_csv(f)
    else:
        df = pd.read_csv(response)

    # Create SQLAlchemy engine
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')

    # Insert data into PostgreSQL table
    df.to_sql(table_name, engine, if_exists='replace', index=False)

if __name__ == "__main__":
    if len(sys.argv) != 8:
        print("Usage: python load_to_postgres.py <url> <dbname> <user> <password> <host> <port> <table_name>")
        sys.exit(1)

    url, dbname, user, password, host, port, table_name = sys.argv[1:]
    load_csv_to_postgres(url, dbname, user, password, host, port, table_name)