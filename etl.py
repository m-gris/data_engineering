import os
import glob
import psycopg2
import utils
import pandas as pd
from sql_queries import *


def process_song_file(cursor, folderpath, file_ext):
    # open song file
    song_files = utils.get_files(folderpath, file_ext)
    
    df = 

    # insert song record
    song_data = 
    cursor.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = 
    cursor.execute(artist_table_insert, artist_data)


def process_log_file(cursor, folderpath):
    # open log file
    df = 

    # filter by NextSong action
    df = 

    # convert timestamp column to datetime
    t = 
    
    # insert time data records
    time_data = 
    column_labels = 
    time_df = 

    for i, row in time_df.iterrows():
        cursor.execute(time_table_insert, list(row))

    # load user table
    user_df = 

    # insert user records
    for i, row in user_df.iterrows():
        cursor.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cursor.execute(song_select, (row.song, row.artist, row.length))
        results = cursor.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = 
        cursor.execute(songplay_table_insert, songplay_data)


def process_data(cursor, connection, folderpath, file_ext, func):
    # get all files matching extension from directory
    all_files = utils.get_files(folderpath, file_ext)

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, folderpath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cursor, datafile)
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