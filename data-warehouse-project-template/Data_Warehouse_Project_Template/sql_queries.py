import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist AS VARCHAR,
        auth AS VARCHAR,
        firstName AS VARCHAR,
        gender as VARCHAR,
        itemInSession AS INT,
        lastName AS VARCHAR,
        length AS NUMERIC,
        level AS VARCHAR,
        location AS VARCHAR,
        method AS VARCHAR,
        page AS VARCHAR,
        registration AS NUMERIC,
        sessionID AS INT,
        song AS VARCHAR,
        status AS INT,
        ts AS BIGINT,
        userAgent AS VARCHAR,
        userId AS INT
    )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs AS INT,
        artist_id AS VARCHAR,
        artist_latitude AS NUMERIC,
        artist_longitude AS NUMERIC,
        artist_location AS VARCHAR,
        artist_name AS VARCHAR,
        song_id AS VARCHAR,
        title AS VARCHAR,
        duration AS NUMERIC,
        year AS INT
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id AS IDENTITY(0,1),
        start_time AS DATETIME,
        user_id AS INT,
        level AS VARCHAR,
        song_id AS VARCHAR,
        artist_id AS VARCHAR,
        session_id AS INT,
        location AS VARCHAR,
        user_agent AS VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id AS INT,
        first_name AS VARCHAR,
        last_name AS VARCHAR,
        gender AS VARCHAR,
        level AS VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id AS VARCHAR,
        title AS VARCHAR,
        artist_id AS VARCHAR,
        year AS INT,
        duration AS NUMERIC
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id AS VARCHAR,
        name AS VARCHAR,
        location AS VARCHAR,
        latitude AS NUMERIC,
        longitude AS NUMERIC
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time as TIMESTAMP,
        hour AS INT,
        day AS INT,
        week AS INT,
        month AS INT,
        year AS INT,
        weekday AS INT
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    timeformat as 'epochmillisecs'
    truncatecolumns blanksasnull emptyasnull
    json 's3://udacity-dend/log_json_path.json';
""").format(DWH_IAM_ARN)

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2';
""").format(DWH_IAM_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  
        events.ts,
        events.userID,
        events.level,
        songs.song_id,
        songs.artist_id,
        events.sessionId,
        events.location,
        events.userAgent
    FROM staging_events AS events
    JOIN staging_songs AS songs
        ON (events.artist = songs.artist_name)
        AND (events.song = songs.title)
        AND (events.length = songs.duration)
        WHERE events.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userID as user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong'
    GROUP BY userID, firstName, lastName, gender, level, ts
    ORDER BY user_id, ts DESC
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT
        song_id,
        title,
        artist_id,
        year,
        duration as numeric
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT
        ts as start_time,
        extract(hour from ts) as hour,
        extract(d from ts) as day, --day of month from 1 to 30/31
        extract(w from ts) as week,
        extract(mon from ts) as month,
        extract(yr from ts) as year,
        extract(weekday from ts) as weekday -- day of week from 0 to 6
    FROM staging_events
    WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
