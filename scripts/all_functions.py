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
import datetime


_author_ = 'vbs'


def get_pg_conn():
    conn = psycopg2.connect(
        dbname='spotify_demo_db',
        user=credentials['pg_user'],
        password=credentials['pg_password'],
        host='localhost',
        port='5432')
    return conn


def connect_apis():
    # Define your Spotify API credentials
    spotify_client_id = credentials['spotify_client_id']
    spotify_client_secret = credentials['spotify_client_secret']
    spotify_redirect_uri = credentials['spotify_redirect_uri']  # This should match your Spotify App's redirect URI

    # Define your Genius API credentials
    genius_client_access_token = credentials['genius_client_access_token']

    # Create a SpotifyOAuth object to authenticate and get access token
    sp_oauth = SpotifyOAuth(client_id=spotify_client_id, client_secret=spotify_client_secret,
                            redirect_uri=spotify_redirect_uri,
                            scope="user-library-read user-read-recently-played user-read-private user-read-email ")

    # Create a Spotipy object with the authenticated SpotifyOAuth object
    sp = spotipy.Spotify(auth_manager=sp_oauth)

    genius = lyricsgenius.Genius(genius_client_access_token)
    return sp, genius


sp, genius = connect_apis()


# ****************************************************************************************************

def get_user_playlists(sp, user_id):
    try:
        # Initialize an empty list to store all playlists
        all_playlists = []

        # Set the initial offset to 0
        offset = 0

        while True:
            # Get playlists with the current offset
            playlists = sp.user_playlists(user_id, offset=offset)

            # If there are no more playlists, break out of the loop
            if not playlists['items']:
                break
            # Extract relevant information from the playlist objects and append to the list
            all_playlists.extend([{'id': playlist['id'], 'name': playlist['name'], 'href': playlist['href'],
                                   'description': playlist['description']}
                                  for playlist in playlists['items']])

            # Increment the offset for the next request
            offset += len(playlists['items'])
        return all_playlists

    except Exception as e:
        print(f"Error: {e}")
        return None


# ****************************************************************************************************

def get_playlist_tracks(sp, playlist_id):
    try:
        # Get playlist tracks using the playlist_tracks endpoint
        playlist_tracks = sp.playlist_tracks(playlist_id)

        # Extract relevant information from the track objects
        tracks_data = []

        for track in playlist_tracks['items']:
            track_info = track['track']
            tracks_data.append({
                'id': track_info['id'],
                'name': track_info['name'],
                'artist': [artist['name'] for artist in track['track']['artists']],  # Assuming the first artist for simplicity
                'album': track_info['album']['name'],
                'duration_ms': track_info['duration_ms'],
                'popularity': track_info['popularity'],
                'preview_url': track_info['preview_url'],
                'track_number': track_info['track_number'],
                'uri': track_info['uri'],
                'is_local': track_info['is_local'],
                'href': track_info['href'],
                'explicit': track_info['explicit'],
                'disc_number': track_info['disc_number'],
            })

        return tracks_data

    except Exception as e:
        print(f"Error: {e}")
        return None



# ****************************************************************************************************

def change_quotes(input_value):
    if isinstance(input_value, str):
        return input_value.replace("'", '"')
    elif isinstance(input_value, int):
        return str(input_value)
    else:
        return input_value


# ****************************************************************************************************

def get_tracks(playlist_id):
    try:
        # Get playlist tracks using the playlist_tracks endpoint
        playlist_tracks = sp.playlist_tracks(playlist_id)

        # Extract relevant information from the track objects
        tracks_data = []

        for track in playlist_tracks['items']:
            try:
                track_info = track['track']
                tracks_data.append({
                    'track_id': track_info['id'],
                    'track_album': track_info['album']['name'],
                    'track_artists': f"{', '.join([artist['name'] for artist in track_info['artists']])}",
                    'track_available_markets': f"[{', '.join([available_markets for available_markets in track_info['available_markets']])}]",
                    'track_disc_number': track_info['disc_number'],
                    'track_duration_ms': track_info['duration_ms'],
                    'track_explicit': track_info['explicit'],
                    'track_href': track_info['href'],
                    'track_is_local': track_info['is_local'],
                    'track_name': track_info['name'],
                    'track_popularity': track_info['popularity'],
                    'track_preview_url': track_info['preview_url'],
                    'track_number': track_info['track_number'],
                    'track_uri': track_info['uri']
                })
            except:
                print(f'Error in trackload')
                traceback.print_exc()

        # 'track_external_ids': track_info['external_ids'],
        # 'track_external_urls': track_info['external_urls'],

        return tracks_data

    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
        return None


# ****************************************************************************************************

def get_albums(playlist_id):
    try:
        # Get playlist tracks using the playlist_tracks endpoint
        playlist_tracks = sp.playlist_tracks(playlist_id)

        # Extract unique album information from the track objects
        albums_data = {}

        for track in playlist_tracks['items']:
            track_info = track['track']
            album = track_info['album']

            # Check if album information is not already collected
            if album['id'] not in albums_data:
                album_data = {
                    'album_id': album['id'],
                    'album_name': album['name'],
                    'album_type': album['album_type'],
                    'album_release_date': album['release_date'],
                    'album_release_date_precision': album['release_date_precision'],
                    'album_total_tracks': album['total_tracks'],
                    'album_artists': [artist['name'] for artist in album['artists']],
                    'album_href': album['href'],
                    'album_uri': album['uri'],
                }

                albums_data[album['id']] = album_data

        return list(albums_data.values())

    except Exception as e:
        print(f"Error: {e}")
        return None


# ****************************************************************************************************

def get_artist_as_list(playlist_id):
    try:
        # Get playlist tracks using the playlist_tracks endpoint
        playlist_tracks = sp.playlist_tracks(playlist_id)

        # Extract unique artist information from the track objects
        artists_list = []

        for track in playlist_tracks['items']:
            track_info = track['track']

            for artist_info in track_info['artists']:
                artists_list.append(artist_info['id'])
        return artists_list

    except Exception as e:
        print(f"Error: {e}")
        return None


# ****************************************************************************************************

def get_all_artitst():
    all_artists = []
    for playlist_id in playlists.keys():
        artists = get_artist_as_list(playlist_id)
        if artists:
            for artist in artists:
                if artist is not None and artist not in all_artists:
                    all_artists.append(artist)


# ****************************************************************************************************

def fetch_result_set(query):
    try:
        conn = get_pg_conn()

        cursor = conn.cursor()
        cursor.execute(query)

        # Fetch all track IDs
        track_ids = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()
        return track_ids

    except Exception as e:
        print(f"Error: {e}")
        return None


# ****************************************************************************************************

def get_audio_features_for_large_set(track_ids, sp):
    try:
        # Initialize an empty list to store all audio features
        all_audio_features = []

        # Split the track IDs into chunks of 100 (maximum allowed per request)
        track_id_chunks = [track_ids[i:i + 100] for i in range(0, len(track_ids), 100)]

        # Fetch audio features for each chunk
        for chunk in track_id_chunks:
            audio_features_chunk = sp.audio_features(chunk)
            all_audio_features.extend(audio_features_chunk)

        return all_audio_features

    except Exception as e:
        print(f"Error fetching audio features: {e}")
        return None


# ****************************************************************************************************

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
        print(f"Error: {e}")
        return None


# ****************************************************************************************************

def get_playlist_artists(playlist_id):
    try:
        # Get playlist tracks using the playlist_tracks endpoint
        playlist_tracks = sp.playlist_tracks(playlist_id)

        # Extract unique artist information from the track objects
        artists_data = {}

        for track in playlist_tracks['items']:
            track_info = track['track']

            for artist_info in track_info['artists']:
                artist_id = artist_info['id']

                # Check if artist information is not already collected
                if artist_id not in artists_data:
                    artist_data = {
                        'id': artist_id,
                        'name': artist_info['name'],
                        'href': artist_info['href'],
                        'uri': artist_info['uri'],
                        'type': artist_info['type'],
                    }

                    artists_data[artist_id] = artist_data

        return list(artists_data.values())

    except Exception as e:
        print(f"Error: {e}")
        return None


# ****************************************************************************************************

def get_artist_details(sp, artist_ids):
    all_genres = []
    # Set the number of artists to request per API call
    batch_size = 50

    # Paginate through the artist IDs
    for offset in range(0, len(artist_ids), batch_size):
        # Extract a batch of artist IDs
        artist_ids_batch = artist_ids[offset:offset + batch_size]

        # Fetch artist details for the current batch
        artists = sp.artists(artist_ids_batch)
        genres = process_artists(artists)

        for genre in genres:
            if genre not in all_genres:
                all_genres.append(genre)

        remaining_artist_ids = len(artist_ids) - (offset + batch_size)
        processed_artist_ids = offset + len(artist_ids_batch)
        print(f"Remaining artist IDs: {remaining_artist_ids}\nProgress: {processed_artist_ids} / {len(artist_ids)}",
              end='\r')

        # Introduce a delay to avoid rate limiting (adjust as needed)
        time.sleep(10)
    return all_genres


# ****************************************************************************************************

def process_artists(artists):
    # Connect to PostgreSQL
    try:
        all_genres = []

        # Open a cursor to perform database operations
        conn = get_pg_conn()
        cursor = conn.cursor()

        # Process each artist and store in the PostgreSQL table
        for artist in artists['artists']:
            for genre in artist['genres']:
                if genre not in all_genres:
                    all_genres.append(genre)

            artist_id = artist['id']
            artist_name = artist['name'].replace("'", "''")
            followers = artist['followers']['total']
            genre_str = ', '.join([genre.replace("'", "''") for genre in artist['genres']])
            genres = f"[{genre_str}]"
            popularity = artist['popularity']

            # Replace 'your_table_name' with your actual table name
            query = f"INSERT INTO artist (artist_id, artist_name, followers, genres, popularity) " \
                    f"VALUES ('{artist_id}', '{artist_name}', {followers}, '{genres}', {popularity}) ON CONFLICT (artist_id) DO NOTHING;"

            cursor.execute(query)

        # Commit the transaction
        conn.commit()
        cursor.close()
        conn.close()
        return all_genres

    except Exception as e:
        print(f"Error processing artists: {e}")


# ****************************************************************************************************

def get_song_lyrics(song_name, artist_name):
    try:
        # Get the first track found on Spotify
        sp, genius = connect_apis()

        # Search for the song on Genius using the artist and title
        genius_results = genius.search_song(song_name, artist_name)
        if genius_results:
            # Print and return the lyrics
            print(f"Lyrics for '{song_name}' by {artist_name}:\n")
            #print(genius_results.lyrics)
            return genius_results.lyrics
        else:
            print(f"Lyrics for '{song_name}' not found on Genius.")
            return None

    except Exception as e:
        print(f"Error: {e}")
        return None


# ****************************************************************************************************

def get_user_details_by_id(user_id, sp):
    try:
        # Retrieve user details using the Spotify Web API
        user_details = sp.user(user_id)

        # Extract relevant information
        details = {
            'display_name': user_details.get('display_name'),
            'id': user_details.get('id'),
            'email': user_details.get('email'),
            'followers': user_details.get('followers', {}).get('total'),
            'country': user_details.get('country'),
        }

        # Get the user's recently played tracks
        recent_tracks = sp.user_playlists(user_details, limit=50)  # Adjust the limit as needed

        # Extract relevant information from recent tracks
        tracks = [{
            'track_name': track['track']['name'],
            'artist_name': track['track']['artists'][0]['name'],
            'played_at': track['played_at'],
            'album_name': track['track']['album']['name'],
        } for track in recent_tracks['items']]

        details['recent_tracks'] = tracks

        return details

    except Exception as e:
        print(f"Error fetching user details: {e}")
        traceback.print_exc()
        return None


# ****************************************************************************************************


def print_user_playlists(user_id, sp):
    try:
        # Initialize an empty list to store all playlists
        all_playlists = []

        # Initial request to get the first batch of playlists
        playlists = sp.user_playlists(user_id)

        # Append the playlists to the list
        all_playlists.extend(playlists['items'])

        # Check for more playlists and continue fetching until none are left
        while playlists['next']:
            playlists = sp.next(playlists)
            all_playlists.extend(playlists['items'])

        # Print the public playlists
        if all_playlists:
            print(f"Public Playlists for user {user_id}:")
            for i, playlist in enumerate(all_playlists, 1):
                print(f"{i}. {playlist['name']} (ID: {playlist['id']})")
        else:
            print(f"No public playlists found for user {user_id}")

    except Exception as e:
        print(f"Error fetching user playlists: {e}")


# ****************************************************************************************************


## find_user details for each user
def get_users_details(sp, user_ids):
    # List to store user details
    users_details = []

    count_insertions = 0

    for user_id in user_ids:
        try:
            conn = get_pg_conn()
            cursor = conn.cursor()

            # Check if the user already exists in the table
            cursor.execute("""
                SELECT 1 FROM user_details WHERE user_id = %s
            """, (user_id,))
            existing_user = cursor.fetchone()

            if existing_user:
                pprint(f"User with ID {user_id} already exists. Skipping insertion.")
            else:
                # Get details for the specified user
                user_info = sp.user(user_id)

                # Append user details to the list
                user_details = {
                    "user_id": user_info['id'],
                    "user_name": user_info['display_name'],
                    "user_followers": user_info['followers']['total'],
                    "user_url": user_info['external_urls']['spotify'],
                    "user_added_on": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                users_details.append(user_details)

                # Insert user details into PostgreSQL table
                cursor.execute("""
                    INSERT INTO user_details (user_id, user_name, user_followers, user_url, user_added_on)
                    VALUES (%s, %s, %s, %s, %s)
                """, (user_details["user_id"],
                      user_details["user_name"],
                      user_details["user_followers"],
                      user_details["user_url"],
                      user_details["user_added_on"]))
                conn.commit()
                pprint(f"User {user_id} added successfully.")
                count_insertions += 1
        except spotipy.SpotifyException as e:
            print(f"Error for user {user_id}: {e}")
            traceback.print_exc()

        finally:
            cursor.close()
            conn.close()
    pprint(f"Added {count_insertions} users.")
    return users_details



## Find all user playlists
def create_playlist_entry(playlist_id, playlist_name, playlist_description, playlist_url, playlist_updated_on):
    """
    Create or update an entry in the 'playlist' table.
    """
    conn = get_pg_conn()
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO playlist (playlist_id, playlist_name, playlist_description, playlist_url, playlist_updated_on)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (playlist_id) DO UPDATE
        SET playlist_name = CASE WHEN playlist.playlist_name <> EXCLUDED.playlist_name THEN EXCLUDED.playlist_name ELSE playlist.playlist_name END,
            playlist_description = CASE WHEN playlist.playlist_description <> EXCLUDED.playlist_description THEN EXCLUDED.playlist_description ELSE playlist.playlist_description END,
            playlist_url = CASE WHEN playlist.playlist_url <> EXCLUDED.playlist_url THEN EXCLUDED.playlist_url ELSE playlist.playlist_url END,
            playlist_updated_on = EXCLUDED.playlist_updated_on
        WHERE playlist.playlist_id = EXCLUDED.playlist_id
    """

    cursor.execute(insert_query, (playlist_id, playlist_name, playlist_description, playlist_url, playlist_updated_on))
    conn.commit()


def create_user_playlist_entry(user_id, playlist_id):
    """
    Create an entry in the 'user_playlists' table.
    """
    conn = get_pg_conn()
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO user_playlist (user_id, playlist_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
    """

    cursor.execute(insert_query, (user_id, playlist_id))
    conn.commit()


def process_user_playlists(user_id):
    """
    Process user playlists and add entries to the 'playlist' and 'user_playlists' tables.
    """
    playlists = sp.user_playlists(user_id)
    total_songs = 0
    for playlist in playlists['items']:
        playlist_id = playlist['id']
        playlist_name = playlist['name']
        playlist_description = playlist['description']
        playlist_url = playlist['external_urls']['spotify']
        playlist_updated_on = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        create_playlist_entry(playlist_id, playlist_name, playlist_description, playlist_url, playlist_updated_on)
        create_user_playlist_entry(user_id, playlist_id)
        playlist_tracks = sp.playlist_tracks(playlist_id)
        total_songs += playlist_tracks['total']
    return total_songs


sp, genius = connect_apis()


def fetch_result_set(query):
    try:
        conn = get_pg_conn()

        cursor = conn.cursor()
        cursor.execute(query)

        # Fetch all track IDs
        track_ids = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        return track_ids

    except Exception as e:
        print(f"Error: {e}")
        return None


# Find track details:
def create_track_entry(track):
    """
    Create an entry in the 'track' table.
    """
    conn = get_pg_conn()
    cur = conn.cursor()
    insert_query = """
        INSERT INTO track (
            track_id, track_album, track_available_markets,
            track_disc_number, track_explicit, track_href, track_is_local,
            track_name, track_preview_url, track_number, track_uri, track_added_on
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (track_id) DO NOTHING
        RETURNING track_id
    """

    cur.execute(insert_query, (
        track['id'], track['album']['name'],
        ', '.join(track['available_markets']), track['disc_number'], track['explicit'],
        track['href'], track['is_local'], track['name'], track['preview_url'],
        track['track_number'], track['uri'], datetime.datetime.now()
    ))
    conn.commit()
    return track['id']

def create_artist_entry(artist):
    """
    Create an entry in the 'artist' table.
    """
    conn = get_pg_conn()
    cur = conn.cursor()
    followers = artist['followers']['total'] if 'followers' in artist else None
    genres = ', '.join(artist['genres']) if 'genres' in artist else None
    popularity = artist['popularity'] if 'popularity' in artist else None

    insert_query = """
        INSERT INTO artist (artist_id, artist_name, followers, genres, popularity, artist_added)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (artist_id) DO NOTHING
        RETURNING artist_id
    """

    cur.execute(insert_query, (
        artist['id'], artist['name'], followers, genres, popularity, datetime.datetime.now()
    ))
    conn.commit()
    return artist['id']

def create_track_artist_entry(track_id, artist_id):
    """
    Create an entry in the 'track_artist' table.
    """
    conn = get_pg_conn()
    cur = conn.cursor()
    insert_query = """
        INSERT INTO track_artist (track_id, artist_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
    """

    cur.execute(insert_query, (track_id, artist_id))
    conn.commit()

def fetch_tracks_from_playlist(playlist_id):
    """
    Fetch all tracks from a playlist and add them to the 'track', 'artist', and 'track_artist' tables with pagination.
    """
    offset = 0
    limit = 50  # Adjust the limit based on your needs
    print(playlist_id, "initiated.")
    while True:
        results = sp.playlist_tracks(playlist_id, offset=offset, limit=limit)

        if not results['items']:
            break

        for item in results['items']:
            track = item['track']
            track_id = create_track_entry(track)

            for artist in track['artists']:
                artist_id = create_artist_entry(sp.artist(artist['id']))
                create_track_artist_entry(track_id, artist_id)

        offset += limit
    print(playlist_id, "completed.")
    print("--")



query = "SELECT DISTINCT playlist_id FROM playlist"
playlists = [playlist[0] for playlist in fetch_result_set(query)]
print(len(playlists))

# user_id_df = pd.read_csv("user_id.csv")
# user_id_list = user_id_df["user_id"]
#
# for user_id in user_id_list:
#     print(user_id, process_user_playlists(user_id))
#
#
# query = "SELECT DISTINCT playlist_id FROM playlist"
# playlists = [playlist[0] for playlist in fetch_result_set(query)]