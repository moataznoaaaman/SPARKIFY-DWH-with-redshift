import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open(r'Data_Warehouse_Project_Template\dwh.cfg'))

LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
role = config.get('IAM_ROLE','ARN')


#AWS Credentials
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
# DROP TABLES
# stage 1

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events "
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

# stage 2

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# stage 1

staging_events_table_create= ("""
CREATE TABLE staging_events
(
    artist varchar(786),
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession integer,
    lastName varchar,
    length double precision,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration double precision,
    sessionId integer,
    song varchar,
    status integer,
    ts bigint,
    userAgent varchar,
    userId varchar
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs
(
    num_songs integer,
    artist_id varchar,
    artist_latitude double precision,
    artist_longitude double precision,
    artist_location varchar(786),
    artist_name varchar(786),
    song_id varchar,
    title varchar,
    duration double precision,
    year integer
)
""")

# stage 2

songplay_table_create = ("""
CREATE TABLE songplays
(
    songplay_id int IDENTITY(0,1),
    start_time timestamp NOT NULL,
    user_id varchar NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id integer,
    location varchar,
    user_agent varchar,
    PRIMARY KEY (songplay_id)
)
""")

# purely from the event table but check if we are only considering users with next page only
user_table_create = ("""
CREATE TABLE users
(
    user_id varchar,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar,
    PRIMARY KEY (user_id)
)
""")
# purely from the songs staging table
song_table_create = ("""
CREATE TABLE songs
(
    song_id varchar,
    title varchar NOT NULL,
    artist_id varchar NOT NULL,
    year int,
    duration double precision NOT NULL,
    PRIMARY KEY (song_id)
)
""")
# purely from the songs staging table
artist_table_create = ("""
CREATE TABLE artists
(
    artist_id varchar,
    name varchar NOT NULL,
    location varchar,
    latitude double precision,
    longitude double precision,
    PRIMARY KEY (artist_id)
)
""")

time_table_create = ("""
CREATE TABLE time
(
    start_time timestamp,
    hour int,
    day int,
    weak int,
    month int,
    year int,
    weekday int,
    PRIMARY KEY (start_time)
)
""")

# STAGING TABLES
 # stage 1

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role=arn:aws:iam::559253790134:role/readfroms3'
region 'us-west-2'
format json as 's3://udacity-dend/log_json_path.json'
dateformat 'auto';
""").format(LOG_DATA)

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role=arn:aws:iam::559253790134:role/readfroms3'
region 'us-west-2'
json 'auto' ;
""").format(SONG_DATA)

# FINAL TABLES

# stage 2

# EVENTS AS E
# SONGS AS S
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second', e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
FROM staging_events e
LEFT JOIN staging_songs s
on e.artist = s.artist_name AND e.song = s.title
where e.page = 'NextSong' AND e.userId IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT ON (userId), firstName, lastName, gender, level
FROM staging_events
WHERE page = 'NextSong' AND userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT ON (song_id), title, artist_id, year, duration
FROM staging_songs;

""")

artist_table_insert = ("""
INSERT INTO artists (artist_id ,name, location, latitude, longitude)
SELECT DISTINCT ON (artist_id), artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, weak, month, year, weekday)
SELECT T1.convstmp, EXTRACT (HOUR FROM T1.convstmp), EXTRACT (DAY FROM T1.convstmp), EXTRACT (WEEK FROM T1.convstmp), EXTRACT (MONTH FROM T1.convstmp), EXTRACT (YEAR FROM T1.convstmp), EXTRACT (WEEKDAY FROM T1.convstmp)
FROM (
SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS convstmp
FROM staging_events
WHERE page = 'NextSong'
) AS T1;
 """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
# insert_table_queries = [songplay_table_insert]

# create_table_queries = [staging_events_table_create, staging_songs_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop]
# copy_table_queries = [staging_events_copy, staging_songs_copy]

# create_table_queries = [staging_songs_table_create]
# drop_table_queries = [staging_songs_table_drop]
# copy_table_queries = [staging_songs_copy]