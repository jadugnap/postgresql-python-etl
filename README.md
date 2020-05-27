# postgresql-python-etl
A demonstration of star schema modeling in PostgreSQL with ETL pipeline in Python.
[https://www.udacity.com/course/data-engineer-nanodegree--nd027]

## Purpose

This ETL pipeline is built to help a startup `Sparkify` analyze what songs the users are listening to and users' activity on their new music streaming app. They will have functionality to query data which resides in JSON logs as well as JSON metadata on the songs.

Star schema is chosen in the Postgres database to optimize queries on song play analysis.

Refer to the bottom for queries and results example.

## Schema

### Fact Table
1. songplays - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
#### 1. users - users in the app
- user_id, first_name, last_name, gender, level
#### 2. songs - songs in music database
- song_id, title, artist_id, year, duration
#### 3. artists - artists in music database
- artist_id, name, location, latitude, longitude
#### 4. time - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday

## Files
- `sql_queries.py` contains sql queries to be imported by the py & ipynb files.
- `create_tables.py` initializes ETL process by dropping and creating tables - `this is the first file to run`.
- `etl.ipynb` contains step-by-step ETL process before developing final etl.py file.
- `etl.py` extracts, transforms, and loads song_data and log_data into tables - `this is the second file to run`.
- `test.ipynb` tests the database state throughout the ETL process - `this is the last file to run`.

## How to run
1. Test the initial state by `test.ipynb` (db not created -- conn error) then restart after complete.
2. Run `python create_tables.py `.
3. Test the middle state by `test.ipynb` (db tables created -- no data) then restart after complete.
4. Run `python etl.py`.
5. Test the final state by `test.ipynb` (data populated).

## How to query - [test.ipynb](https://github.com/jadugnap/postgresql-python-etl/blob/master/test.ipynb)
```
# Connect to database
%load_ext sql
%sql postgresql://student:student@127.0.0.1/sparkifydb

# Top 10 users with highest songplay counts
%sql SELECT user_id,COUNT(songplay_id) FROM songplays GROUP BY user_id ORDER BY count DESC LIMIT 10;
```

![queries and results example](https://github.com/jadugnap/postgresql-python-etl/blob/master/postgresql-query-result.png)
