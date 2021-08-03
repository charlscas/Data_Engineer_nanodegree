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
    artist          VARCHAR,
    auth            VARCHAR,
    firstName       VARCHAR,
    gender          VARCHAR,
    itemInSession   INT,
    lastName        VARCHAR,
    length          DECIMAL,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    INT,
    sessionId       INT,
    song            VARCHAR,
    status          INT,
    ts              INT,
    userAgent       VARCHAR,
    userId          INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INT,
    artist_id           VARCHAR,
    artist_latitude     DECIMAL,
    artist_longitude    DECIMAL,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            DECIMAL,
    year                INT    
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id     INT         IDENTITY(0,1)    PRIMARY KEY, 
    start_time      TIMESTAMP   NOT NULL         REFERENCES time(start_time), 
    user_id         INT         NOT NULL, 
    level           VARCHAR, 
    song_id         VARCHAR     NOT NULL, 
    artist_id       VARCHAR     NOT NULL, 
    session_id      INT, 
    location        VARCHAR, 
    user_agent      VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id         VARCHAR     PRIMARY KEY, 
    first_name      VARCHAR, 
    last_name       VARCHAR, 
    gender          VARCHAR, 
    level           VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id         VARCHAR     PRIMARY KEY, 
    title           VARCHAR     SORTKEY,     
    artist_id       VARCHAR     NOT NULL    REFERENCES artists(artist_id), 
    year            INT,     
    duration        DECIMAL
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id       VARCHAR     PRIMARY KEY, 
    name            VARCHAR     SORTKEY,     
    location        VARCHAR,     
    latitude        DECIMAL,     
    longitude       DECIMAL
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time      TIMESTAMP   PRIMARY KEY     DISTKEY     SORTKEY, 
    hour            INT         NOT NULL,
    day             INT         NOT NULL, 
    week            INT         NOT NULL, 
    month           INT         NOT NULL, 
    year            INT         NOT NULL, 
    weekday         VARCHAR     NOT NULL
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
