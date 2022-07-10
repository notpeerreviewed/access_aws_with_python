import configparser
import boto3
import create_redshift as cr


myClusterProps = cr.redshift.describe_clusters(ClusterIdentifier=cr.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

# If statement to avoid querying cluster if status is not 'available'
if myClusterProps['ClusterStatus'] == 'available':
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
else:
    print("Cluster not currently available. Please check status before proceeding.")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (
artist text,
auth text,
firstName text,
gender text,
itemInSession INT,
lastName text,
length NUMERIC,
level text,
location text,
method text,
page text,
registration NUMERIC,
sessionID INT,
song text,
status INT,
ts BIGINT,
userAgent text,
userId INT);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
num_songs INT,
artist_id text,
artist_latitude NUMERIC,
artist_longitude NUMERIC,
artist_location text,
artist_name text,
song_id text,
title text,
duration float,
year INT);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
songplay_id INT IDENTITY(0,1),
start_time timestamp,
user_id INT,
level text,
song_id text,
artist_id text,
session_id INT,
location text,
user_agent text
);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
user_id INT,
first_name text,
last_name text,
gender text,
level text);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
song_id text,
title text,
artist_id text,
year INT,
duration NUMERIC);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
artist_id text,
name text,
location text,
latitude NUMERIC,
longitude NUMERIC);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
start_time TIMESTAMP,
hour INT,
day INT,
week  INT,
month INT,
year INT,
weekday INT);""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from 's3://udacity-dend/log_data'
credentials 'aws_iam_role={}'
region 'us-west-2'
truncatecolumns blanksasnull emptyasnull
json 's3://udacity-dend/log_json_path.json';
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song_data'
credentials 'aws_iam_role={}'
format as json 'auto'
region 'us-west-2';
""").format(DWH_ROLE_ARN)

# FINAL TABLES
# ref https://knowledge.udacity.com/questions/851543 for help with the time conversion
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + (events.ts / 1000) * INTERVAL '1 second' as start_time,
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

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
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

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
song_id,
title,
artist_id,
year,
duration as numeric
FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT
artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude
FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT a.start_time,
EXTRACT (HOUR FROM a.start_time), EXTRACT (DAY FROM a.start_time),
EXTRACT (WEEK FROM a.start_time), EXTRACT (MONTH FROM a.start_time),
EXTRACT (YEAR FROM a.start_time), EXTRACT (WEEKDAY FROM a.start_time) 
FROM
(SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time FROM staging_events) a;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
