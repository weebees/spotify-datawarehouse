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


def sql_create_table(create_sql_table):
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute(create_sql_table)
    conn.commit()


def drop_all_tables(database_name, conn):
    try:
        cursor = conn.cursor()
        # Get a list of all tables in the current schema
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        tables = cursor.fetchall()

        # Drop each table
        for table in tables:
            table_name = table[0]
            drop_table_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table_name))
            cursor.execute(drop_table_query)
            print(f"Dropped table: {table_name}")

        # Commit changes and close the connection
        conn.commit()
        print("All tables dropped successfully.")
    except Exception as e:
        print(f"Error: {e}")


list_of_tables = [

    '''CREATE TABLE IF NOT EXISTS user_details (
            user_id VARCHAR(255) PRIMARY KEY,
            user_name VARCHAR(255),
            user_followers INTEGER,
            user_url VARCHAR(255),
            user_added_on TIMESTAMP
            )
    ''',

    '''CREATE TABLE IF NOT EXISTS playlist (
            playlist_id VARCHAR(255) PRIMARY KEY,
            playlist_name VARCHAR(255),
            playlist_description TEXT,
            playlist_url VARCHAR(255),
            playlist_updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
    ''',

    '''CREATE TABLE IF NOT EXISTS user_playlist (
            user_playlist_id SERIAL PRIMARY KEY,
            user_id VARCHAR(255) REFERENCES user_details(user_id), 
            playlist_id VARCHAR(255) REFERENCES playlist(playlist_id),

            CONSTRAINT unique_user_playlist_combination UNIQUE (user_id, playlist_id)
            )
    ''',

    '''CREATE TABLE IF NOT EXISTS track (      
            track_id VARCHAR(255) PRIMARY KEY,
            track_album VARCHAR(255),
            track_artists VARCHAR(255),
            track_available_markets TEXT,
            track_disc_number INTEGER,
            track_explicit BOOLEAN,
            track_href VARCHAR(255),
            track_is_local BOOLEAN,
            track_name VARCHAR(255),
            track_preview_url VARCHAR(255),
            track_number INTEGER,
            track_uri VARCHAR(255),
            track_added_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
    ''',

    '''CREATE TABLE IF NOT EXISTS playlist_track (
            playlist_track_id SERIAL PRIMARY KEY,
            playlist_id VARCHAR(255) REFERENCES playlist(playlist_id),
            track_id VARCHAR(255) REFERENCES track(track_id)
            )
    ''',

    '''CREATE TABLE IF NOT EXISTS album (
            album_id VARCHAR(255) PRIMARY KEY,
            album_name VARCHAR(255),
            album_type VARCHAR(255),
            album_total_tracks INTEGER,
            album_artists TEXT,
            album_href VARCHAR(255),
            album_uri VARCHAR(255),
            album_release_date TIMESTAMP
            )
    ''',

    '''CREATE TABLE IF NOT EXISTS artist (
            artist_id VARCHAR(255) PRIMARY KEY,
            artist_name VARCHAR(255) NOT NULL,
            followers INT,
            genres TEXT,
            popularity INT,
            artist_added TIMESTAMP
            )
    ''',

    '''CREATE TABLE IF NOT EXISTS lyrics (
            track_id VARCHAR(255) PRIMARY KEY REFERENCES track(track_id),
            track_lyrics TEXT
            )
    ''',

    '''CREATE TABLE IF NOT EXISTS track_artist (
            track_artist_id SERIAL PRIMARY KEY,
            track_id VARCHAR(255) REFERENCES track(track_id),
            artist_id VARCHAR(255) REFERENCES artist(artist_id)
            )
    ''',

    '''CREATE TABLE IF NOT EXISTS genre (
            genre_name VARCHAR(255) PRIMARY KEY
            )
    ''',

    '''CREATE TABLE IF NOT EXISTS artist_genre (
            artist_genre_id SERIAL PRIMARY KEY,
            artist_id VARCHAR(255) REFERENCES artist(artist_id),
            genre_name VARCHAR(255) REFERENCES genre(genre_name)
            )
    ''',

    '''CREATE TABLE IF NOT EXISTS audio_features (
            track_id VARCHAR(255) PRIMARY KEY,
            track_popularity DECIMAL,
            track_duration_ms DECIMAL
            )
    ''',

    '''CREATE TABLE IF NOT EXISTS album_artist (
            album_artist_id SERIAL PRIMARY KEY,
            album_id VARCHAR(255) REFERENCES album(album_id),
            artist_id VARCHAR(255) REFERENCES artist(artist_id)
            )
    ''',

    '''CREATE TABLE IF NOT EXISTS album_track (
            album_artist_id SERIAL PRIMARY KEY,
            album_id VARCHAR(255) REFERENCES album(album_id),
            track_id VARCHAR(255) REFERENCES track(track_id)
            )
    ''',

]

# '''CREATE TABLE IF NOT EXISTS audio_features (
#             track_id VARCHAR(255) PRIMARY KEY,
#             track_popularity DECIMAL,
#             track_duration_ms DECIMAL,
#             danceability INT,
#             energy INT,
#             key INT,
#             loudness INT,
#             mode INT,
#             speechiness INT,
#             acousticness INT,
#             instrumentalness INT,
#             liveness INT,
#             valence INT,
#             tempo INT,
#             duration_ms INT,
#             time_signature INT
#             )
#     ''',

for table in list_of_tables:
    sql_create_table(table)
