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
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length decimal,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration int,
    sessionId int,
    song varchar,
    status int,
    ts int,
    userAgent varchar,
    userId int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int NOT NULL,
    artist_id varchar NOT NULL,
    artist_latitude decimal,
    artist_longitude decimal,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration decimal,
    year int    
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1), 
    start_time timestamp NOT NULL REFERENCES time(start_time), 
    user_id int NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar PRIMARY KEY, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar NOT NULL REFERENCES artists(artist_id), 
    year int, 
    duration decimal
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name varchar, 
    location varchar, 
    latitude decimal, 
    longitude decimal
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY, 
    hour int NOT NULL,
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday varchar NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM 's3://udacity-dend/log_data'
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF
    REGION 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    TRUNCATECOLUMNS blankasnull emptyasnull
    JSON 's3://udacity-dend/log_json_path.json'
""").format(config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM 's3://udacity-dend/song_data/A/A/A'
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto'
    COMPUPDATE OFF
    REGION 'us-west-2'
""").format(config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS
create_table_queries = [
    staging_events_table_create, 
    staging_songs_table_create, 
    time_table_create, 
    user_table_create, 
    artist_table_create, 
    songplay_table_create, 
    song_table_create
]
drop_table_queries = [
    staging_events_table_drop, 
    staging_songs_table_drop, 
    song_table_drop, 
    songplay_table_drop, 
    artist_table_drop, 
    user_table_drop, 
    time_table_drop
]
copy_table_queries = [
    staging_events_copy, 
    staging_songs_copy
]
insert_table_queries = [
    songplay_table_insert, 
    user_table_insert, 
    song_table_insert, 
    artist_table_insert, 
    time_table_insert
]
