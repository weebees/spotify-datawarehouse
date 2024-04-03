import pandas as pd
import numpy as np
import requests
import os
import sys
import time
import lyricsgenius
import re
import traceback
import time
import psycopg2
from psycopg2 import sql
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from pprint import pprint
import logging


_author_ = 'vbs'


def change_quotes(input_value):
    if isinstance(input_value, str):
        return input_value.replace("'", '"')
    elif isinstance(input_value, int):
        return str(input_value)
    else:
        return input_value


def get_column_names(table_name):
    try:
        # Connect to the PostgreSQL database
        conn = get_pg_conn()
        cursor = conn.cursor()

        # Get the column names from the information schema
        query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"
        cursor.execute(query)

        # Fetch all column names
        column_names = [row[0] for row in cursor.fetchall()]

        # Close the cursor and connection
        cursor.close()
        conn.close()

        return column_names

    except Exception as e:
        import traceback
        import psycopg2
        import logging
        from psycopg2 import sql
        from pprint import pprint

        from credentials import credentials

        _author_ = 'vbs'

        def get_pg_conn():
            conn = psycopg2.connect(
                dbname='spotify_demo_db',
                user=credentials['pg_user'],
                password=credentials['pg_password'],
                host='localhost',
                port='5432')
            return conn


def print_table_columns(conn, table_name):
    with conn.cursor() as cursor:
        # Query to get the column names for a given table
        query = sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_name = %s;")
        cursor.execute(query, (table_name,))

        # Fetch all rows
        columns = cursor.fetchall()

        # Print the columns
        print(f"Table '{table_name}'. Columns for table '{table_name}")
        for column in columns:
            print(column[0])
        print()


def main():
    # Replace these with your actual database connection details
    conn = get_pg_conn()

    try:
        with conn.cursor() as cursor:
            # Query to get the list of tables in the database
            query = sql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            cursor.execute(query)

            # Fetch all rows
            tables = cursor.fetchall()

            # Iterate over tables and print columns
            for table in tables:
                print_table_columns(conn, table[0])
    finally:
        # Close the database connection
        conn.close()


if __name__ == "__main__":
    main()
