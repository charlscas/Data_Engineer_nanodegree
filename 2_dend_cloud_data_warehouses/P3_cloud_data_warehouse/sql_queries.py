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
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          DECIMAL,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    BIGINT,
    sessionId       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              TIMESTAMP,
    userAgent       VARCHAR,
    userId          VARCHAR
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
    user_id         VARCHAR     NOT NULL, 
    level           VARCHAR, 
    song_id         VARCHAR, 
    artist_id       VARCHAR, 
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
    FROM {log_data_bucket}
    CREDENTIALS 'aws_iam_role={arn}'
    EMPTYASNULL
    BLANKASNULL
    COMPUPDATE OFF
    REGION 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    JSON {log_data_path}
""").format(
        log_data_bucket=config['S3']['LOG_DATA'],
        arn=config['IAM_ROLE']['ARN'],
        log_data_path=config['S3']['LOG_JSONPATH']
        )

staging_songs_copy = ("""
    COPY staging_songs
    FROM 's3://udacity-dend/song_data/A/A/A'
    CREDENTIALS 'aws_iam_role={}'
    EMPTYASNULL
    BLANKASNULL
    REGION 'us-west-2'
    FORMAT AS JSON 'auto'
    COMPUPDATE OFF
""").format(config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent
    )
    SELECT
        e.ts,
        e.userId,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM staging_events AS e
    LEFT JOIN staging_songs AS s
        ON (e.artist = s.artist_name AND e.song = s.title)
    WHERE e.page = 'NextSong' AND s.title IS NOT NULL

""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        e.userId,
        e.firstName,
        e.lastName,
        e.gender,
        e.level
    FROM staging_events AS e
    WHERE e.userId IS NOT NULL
    ON CONFLICT (user_id)
    DO NOTHING;
""")
        # UPDATE 
        # SET first_name = EXCLUDED.first_name, 
        #     last_name  = EXCLUDED.last_name, 
        #     gender     = EXCLUDED.gender,
        #     level      = EXCLUDED.level

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        s.song_id,
        s.title,
        s.artist_id,
        s.year,
        s.duration
    FROM staging_songs AS s
    WHERE s.song_id IS NOT NULL AND s.artist_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        s.artist_id,
        s.artist_name,
        s.artist_location,
        s.artist_latitude,
        s.artist_longitude
    FROM staging_songs AS s
    WHERE s.artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT
        t.start_time,
        EXTRACT(HOUR FROM t.start_time),
        EXTRACT(DAY FROM t.start_time),
        EXTRACT(WEEK FROM t.start_time),
        EXTRACT(MONTH FROM t.start_time),
        EXTRACT(YEAR FROM t.start_time),
        CASE 
            WHEN EXTRACT(DOW FROM t.start_time) == 0 THEN 'Sunday'
            WHEN EXTRACT(DOW FROM t.start_time) == 1 THEN 'Monday'
            WHEN EXTRACT(DOW FROM t.start_time) == 2 THEN 'Tuesday'
            WHEN EXTRACT(DOW FROM t.start_time) == 3 THEN 'Wednesday'
            WHEN EXTRACT(DOW FROM t.start_time) == 4 THEN 'Thursday'
            WHEN EXTRACT(DOW FROM t.start_time) == 5 THEN 'Friday'
            WHEN EXTRACT(DOW FROM t.start_time) == 6 THEN 'Saturday'
            ELSE 'Unknown'
        END,
    
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
