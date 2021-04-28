import os
import glob
import psycopg2
import utils
import pandas as pd
from sql_queries import *


def process_song_file(cursor, filepath):
    
    # open song file    
    df = pd.read_json(filepath, lines = True)

    # get song data
    song_data = list(*df[['song_id', 'title', 'artist_id', 'year', 'duration']].values)
    
    # insert song data
    try:
        cursor.execute(songs_table_insert, song_data)
    except psycopg2.Error as e:
        error_message = 'ERROR: Could not insert this song data:\n' 
        print(song_data)
        print(e)
    
    # get artist record
    artist_data = list(*df[['artist_id', 'artist_name', 
                            'artist_location', 'artist_latitude', 'artist_longitude']].values)
    # insert artist data
    try:
        cursor.execute(artists_table_insert, artist_data)
    except psycopg2.Error as e:
        print('ERROR: Could not insert this artist data:')
        print(artist_data)
        print(e)


def process_log_file(cursor, filepath):
    
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df.query('page == "NextSong"')

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit = 'ms') 
    
    # Extract the timestamp, hour, day, week of year, month, year, and weekday from the ts column 
    start_time = (t.copy()).astype(int)/1000000
    
#     if len(str(start_time)) == 19:
#         raise "timestamp 19 digits long instead of 13 expected by database"
    
    hour = t.dt.hour
    day = t.dt.day
    week = t.dt.weekofyear
    month = t.dt.month
    year = t.dt.year
    weekday = t.dt.weekday


    time_vars = [start_time, hour, day, week, month, year, weekday]
    time_data = [series.values.tolist() for series in time_vars ]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    
    # insert time data records
    for i, row in time_df.iterrows():
        try:
            cursor.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print('ERROR: Could not insert this time data record:')
            print(list(row))
            print(e)

    # load user table
    # Nota Bene: 
    # We dropped duplicates & missing values to abide by the constraints of user_id being PRIMARY KEY 
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates().dropna()

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cursor.execute(users_table_insert, list(row))
        except psycopg2.Error as e:
            print('ERROR: Could not insert this user data record:')
            print(list(row))
            print(e)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cursor.execute(song_select, (row.song, row.artist, row.length))
        results = cursor.fetchone()
        
        if results:
            songid, artistid, _ = results
        else:
            songid, artistid = None, None

        
        
        # Select song_play data
        timestamp = row['ts']
        
        if len(str(timestamp)) == 19:
            raise "timestamp 19 digits long instead of 13 expected by database"

        # Some userIDs were missing. 
        # This catch should allow us to "ignore" rows that contain them.
        try:
            user_id = int(row['userId'])
        except ValueError:
            break

        level = row['level']
        session_id = row['sessionId']
        location = row['location']
        user_agent = row['userAgent']

        #and set to songplay_data
        songplay_data = (timestamp, user_id, level, 
                         songid, artistid, session_id , 
                         location, user_agent)    

        # insert songplay record
        try:
            cursor.execute(songplays_table_insert, songplay_data)
        except psycopg2.Error as e:
            print('ERROR: Could not insert this songplay record:')
            print(songplay_data)
            print(e)


def process_data(cursor, connection, folderpath, file_ext, func):
    # get all files matching extension from directory
    all_files = utils.get_files(folderpath, file_ext)

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, folderpath))

    # iterate over files and process
    for i, filepath in enumerate(all_files, 1):
        func(cursor, filepath)
        connection.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    connection = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cursor = connection.cursor()

    process_data(cursor, connection, 
                 folderpath='data/song_data', file_ext = '.json', 
                 func=process_song_file)
    
    process_data(cursor, connection, 
                 folderpath='data/log_data', file_ext = '.json',
                 func=process_log_file)
    
    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()