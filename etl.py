#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 00:46:47 2019

@author: anthonywa
"""

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):

"""
    This procedure processes a song file whose file path has been provided as an argument.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file

"""
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].iloc[0].values
    song_data[3]=int(song_data[3])
    song_data[4]=float(song_data[4])
    song_data = tuple(song_data)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].iloc[0].values
    artist_data[3] = float(artist_data[3])
    artist_data[4] = float(artist_data[4])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):


"""
    This procedure processes a songplay log file whose file path has been provided as an argument.
    It extracts the songs play activity information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the songplay log file

"""
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = list((t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday))
    column_labels = ('timestamp','hour','day','week_of_year','month','year','weekday')
    time_df = pd.DataFrame(dict((column_labels[i],time_data[i]) for i in range(0,7)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index,pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, row.sessionId, row.location, row.userAgent, songid, artistid)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):

"""
    This procedure processes all data files under one path, the file path has been provided as an argument.
    It iterates through all the files in the folder and combine the data frame.
    

    INPUTS: 
    * cur the cursor variable
    * conn the connection created previously 
    * filepath the file path to the songplay log file
    * func is the process function used to process data

"""

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    print(song_select)
    main()