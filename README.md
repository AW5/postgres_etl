# postgres_etl
for DE Nano Degree

## Project Background
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Database Schema

Five table were created including:

#### Fact Table (1 table)
**songplays** - records in log data associated with song plays 
Usage: i.e. records with page NextSong
Variables: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
#### Dimension Tables (4 table)
**users**
Usage: users in the app
Variables: user_id, first_name, last_name, gender, level
**songs**
Usage: songs in music database
Variables:song_id, title, artist_id, year, duration
**artists** 
Usage: artists in music database
Variables: artist_id, name, location, latitude, longitude
**time**
Usage: timestamps of records in songplays broken down into specific units
Variables: start_time, hour, day, week, month, year, weekday

## Data Pipeline

The Pipeline contains three python files:

- sql_queries.py - All the queries for create, drop and update table are here
- create_tables.py - Runs queries to create empty tables
- etl.py - dump data to the tables created

## How to run

- 1. run create_tables.py in console first to create database and tables
- 2. run etl.py in console to dump data
- Make sure to disconnect from database when done to avoid multi-connecting database

## Use Case

*Using below query to get the top favored artist by Female user, favor here is defined by time played*
>SELECT a.name, COUNT(a.name) cnt FROM songplays sp JOIN (SELECT * FROM users WHERE gender='F')u ON sp.user_id=u.user_id LEFT JOIN artists a ON a.artist_id=sp.artist_id GROUP BY a.name ORDER BY cnt LIMIT 1
