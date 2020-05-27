import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Read json from filepath.
    Extract song_data ["song_id", "title", "artist_id", "year", "duration"]
    Extract artist_data ["id", "name", "location", "latitude", "longitude"]
    Load into songs' and artists' tables of sparkifydb.

    Arguments:
        cur: cursor object from https://www.psycopg.org/docs/cursor.html.
        filepath: path to json files for song_data and artist_data.
    Returns:
        None
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[
        ["song_id", "title", "artist_id", "year", "duration"]
    ].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[
        [
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude"
        ]
    ].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Read json from filepath.
    Extract time_data ["ts", "hr", "day", "week", "mth", "year", "dayofweek"]
    Extract user_data = ["userId", "firstName", "lastName", "gender", "level"]
    Extract & transform songplay_data = [
        "index",
        "ts",
        "userId",
        "level",
        "songid",
        "artistid",
        "sessionId",
        "location",
        "userAgent"
    ]
    Load into songs' and artists' tables of sparkifydb.

    Arguments:
        cur: cursor object from https://www.psycopg.org/docs/cursor.html.
        filepath: path to json files for time_data and user_data.
    Returns:
        None
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query("page == 'NextSong'")

    # convert timestamp column to datetime
    # df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.loc[:, "ts"] = pd.to_datetime(df["ts"], unit="ms")
    t = df["ts"]
    
    # insert time data records
    time_data = [(
        t1,
        t1.hour,
        t1.day,
        t1.weekofyear,
        t1.month,
        t1.year,
        t1.dayofweek
    ) for t1 in t]

    column_labels = (
        "ts", "hour", "day", "weekofyear", "month", "year", "dayofweek"
    )
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        # songid, artistid = results if results else None, None
        results = cur.fetchone()
        songid = results[0] if results else None
        artistid = results[1] if results else None

        # insert songplay record
        songplay_data = [
            index,
            row.ts,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent
        ]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Wrap the functions which are processing log and song files.
    Iterate over each individual file in each function.

    Arguments:
        cur: object from https://www.psycopg.org/docs/cursor.html.
        conn: object from https://www.psycopg.org/docs/connection.html.
        filepath: path to json files for song, artist, time, user.
        func: processor function to be wrapped.
    Returns:
        None
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
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()