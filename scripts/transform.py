from psycopg2 import sql
from pprint import pprint
import psycopg2
import pandas as pd
from profanity_check import predict

from credentials import credentials

_author_ = 'vbs'


def get_pg_conn():
    conn = psycopg2.connect(
        dbname='spotify_staging_db',
        user=credentials['pg_user'],
        password=credentials['pg_password'],
        host='localhost',
        port='5432')
    return conn


def run_sql(query):
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()


def calc_profanity():
    # Step 1: Fetch data from PostgreSQL table into a Pandas DataFrame
    conn = get_pg_conn()

    query = 'SELECT * FROM lyrics;'
    df = pd.read_sql_query(query, conn)

    # Step 2: Apply the function on the desired column
    def count_swear_words(text):
        count = -1
        try:
            words = text.split()
            predictions = predict(words)
            count = sum(predictions)
        except:
            pass
        return count

    df['profanity'] = df['track_lyrics'].apply(count_swear_words)

    # Step 3: Update the PostgreSQL table with the modified DataFrame
    cur = conn.cursor()

    # Update the table with the modified DataFrame
    for index, row in df.iterrows():
        cur.execute(
            'UPDATE lyrics SET profanity = %s WHERE track_id = %s;',
            (row['profanity'], row['track_id'])
        )

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()


calc_profanity()

query = """
    INSERT INTO profanity_analysis_fact (playlist_id, playlist_name, playlist_description, update_on, profanity_score, time_id)
    SELECT
        p.playlist_id,
        p.playlist_name,
        p.playlist_description,
        p.update_on,
        -- Assuming you have a profanity score column in your source data
        source_data.profanity_score,
        t.time_id
    FROM
        source_data
    JOIN
        time_dim t ON DATE_TRUNC('day', source_data.timestamp) = t.date
    JOIN
        playlist_dim p ON source_data.playlist_id = p.playlist_id;
"""

# run_sql(query)