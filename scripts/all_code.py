import pandas as pd
import random
from datetime import datetime, timedelta

_author_ = 'vbs'


def user_activity():
    # Load the CSV file into a DataFrame
    csv_file_path = 'playlist_track.csv'
    df = pd.read_csv(csv_file_path)

    # Set the number of records to pick and the number of subsets
    records_to_pick = [900, 1112, 1820, 2177, 4306, 211, 198, 317]
    users = [
        'hpuzeducv3xo1a831qgw84vap',
        '313v3mo6bvh5cihtfmz6pvkm3rhy',
        'hf9ry3zgffalheg7ucrayg9tz',
        '31b6xt4erwdqhdzkne5ct57nzfm4',
        '1261873101',
        '21ehubo72mwp25kyaf7ap5ufi',
        'd141n7rke9bt3048fq2cspljq',
        'qcgfvdv2oezedi2req0mzc389',
    ]
    num_subsets = 8

    # Set the date range for the timestamps
    start_date = datetime(2023, 11, 25)
    end_date = datetime(2023, 12, 12)

    subsets = []

    # Create 8 subsets
    for subset_number in range(num_subsets):
        # Randomly select 250 records with replacement
        subset = df.sample(n=records_to_pick[subset_number], replace=True)
        subset['user_id'] = users[subset_number]

        # Generate random timestamps between start_date and end_date excluding 2 AM to 10 AM
        subset['timestamp'] = [start_date + timedelta(days=random.randint(0, (end_date - start_date).days),
                                                         seconds=random.randint(36000, 79200))  # Excludes 2 AM to 10 AM
                               for _ in range(records_to_pick[subset_number])]

        # Sort the DataFrame by the timestamp column
        subset.sort_values(by='timestamp', inplace=True)

        # Append the subset to the list
        subsets.append(subset)

    # Concatenate all subsets into a single DataFrame
    combined_df = pd.concat(subsets, ignore_index=True)

    # Sort the combined DataFrame by the timestamp column
    combined_df.sort_values(by='timestamp', inplace=True)

    # Save the combined DataFrame to a CSV file
    combined_df.to_csv('user_activity/user_activity.csv', index=False)


def second_granular_data1():
    # Set the start and end dates
    start_date = datetime(2023, 11, 25)
    end_date = datetime(2023, 12, 13)

    data = []

    # Generate timestamps with every-second granularity
    current_date = start_date
    while current_date <= end_date:
        data.append({
            'day': current_date.day,
            'month': current_date.month,
            'year': current_date.year,
            'hour': current_date.hour,
            'minute': current_date.minute,
            'timestamp': current_date.strftime('%Y-%m-%d %H:%M')
        })

        current_date += timedelta(minutes=1)

    # Create a DataFrame from the generated data
    df = pd.DataFrame(data)

    # Save the DataFrame to a CSV file
    df.to_csv('user_activity/time_data.csv', index=False)


def day_granular_data1():
    # Set the start and end dates
    start_date = datetime(2023, 11, 25)
    end_date = datetime(2023, 12, 13)

    data = []

    # Generate timestamps with every-second granularity
    current_date = start_date
    while current_date <= end_date:
        data.append({
            'day': current_date.day,
            'month': current_date.month,
            'year': current_date.year,
            'timestamp': current_date.strftime('%Y-%m-%d %H:%M')
        })

        current_date += timedelta(days=1)

    # Create a DataFrame from the generated data
    df = pd.DataFrame(data)

    # Save the DataFrame to a CSV file
    df.to_csv('user_activity/day_data.csv', index=False)


def second_granular_data():
    # Set the start and end dates
    start_date = datetime(2023, 11, 25)
    end_date = datetime(2023, 12, 13)

    # Create a list to store time information
    time_info = []
    conn = psycopg2.connect(
        host="localhost",
        database="spotify_demo_db",
        user=credentials['pg_user'],
        password=credentials['pg_password']
    )
    cursor = conn.cursor()
    # Generate time information at every second
    current_date = start_date
    while current_date <= end_date:
        insert_query = sql.SQL("""
        INSERT INTO time_data (day, month, year, hour, minute, second, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """)
        cursor.execute(insert_query, (
            current_date.day,
            current_date.month,
            current_date.year,
            current_date.hour,
            current_date.minute,
            current_date.second,
            current_date
        ))
        current_date += timedelta(seconds=1)
        time_info.append([
            current_date.day,
            current_date.month,
            current_date.year,
            current_date.hour,
            current_date.minute,
            current_date.second,
            current_date.strftime('%Y-%m-%d %H:%M:%S')
        ])
        current_date += timedelta(seconds=1)

    # Create a DataFrame from the time information
    columns = ['day', 'month', 'year', 'hour', 'minute', 'second', 'timestamp']
    df = pd.DataFrame(time_info, columns=columns)

    # Save the DataFrame to a CSV file
    df.to_csv('user_activity/time_data.csv', index=False)


# user_activity()

# Filling Time and Day dimensions
second_granular_data1()
day_granular_data1()

#
