from pyspark.sql import SparkSession
import os
import configparser
from botocore.exceptions import ClientError
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType)
import boto3


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ["AWS_REGION"]= config['AWS']['AWS_REGION']


def create_spark_session():
    """
    Creates Spark session.
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    print("- Spark session created.")
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song data file and saves it into a songs and artists parquet files.
    
    INPUT:
    - spark: a Spark session
    - input_data: S3 bucket name to read data from. Ex: 's3a://udacity-dend/'
    - output_data: S3 bucket name to store the output data. Ex: 's3a://sparkify-analytics-with-spark/'
    """
    
    # get filepath to song data file
    song_data = input_data +  'song_data/A/A/A/*.json'
    
    # read song data file
    song_data_schema = StructType([
        StructField("artist_id", StringType(), False),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), False),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("duration", DoubleType(), False),
        StructField("year", IntegerType(), False)
    ])
    df = spark.read.json(input_data, schema=song_data_schema)
    df.createOrReplaceTempView("song_data")
    print("- Imported song_data and saved as a temp view: 'song_data'.")

    # extract columns to create songs table
    songs_table = spark.sql('''
        SELECT DISTINCT
            s.song_id,
            s.title,
            s.artist_id,
            s.year,
            s.duration
        FROM song_data AS s
        WHERE s.song_id IS NOT NULL AND s.artist_id IS NOT NULL
    ''')
    print("- Created songs_table.")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        output_data + "songs.parquet",
        mode="overwrite"
    )
    print("- Saved parquet file for songs.")

    # extract columns to create artists table
    artists_table = spark.sql('''
        SELECT DISTINCT
            s.artist_id,
            s.artist_name,
            s.artist_location,
            s.artist_latitude,
            s.artist_longitude
        FROM song_data AS s
        WHERE s.artist_id IS NOT NULL
    ''')
    print("- Created artists_table.")
    
    # write artists table to parquet files
    artists_table.write.parquet(
        output_data + "artists.parquet",
        mode="overwrite"
    )
    print("- Saved parquet file for artists.")


def process_log_data(spark, input_data, output_data):
    """
    Process log data file and saves it into a users, time and songplays parquet files.
    
    INPUT:
    - spark: a Spark session
    - input_data: S3 bucket name to read data from. Ex: 's3a://udacity-dend/'
    - output_data: S3 bucket name to store the output data. Ex: 's3a://sparkify-analytics-with-spark/'
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/2018-11-01-events.json'

    # read log data file
    log_data_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), False),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), False),
        StructField("song", StringType(), True),
        StructField("ts", DoubleType(), False),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True)
    ])
    df = spark.read.json(log_data, schema=log_data_schema)
    df.createOrReplaceTempView("event_data")
    print("- Imported log_data and saved as a temp view: 'event_data'.")
    
    # filter by actions for song plays
    df = df.filter(col("page") == "NextSong")

    # extract columns for users table    
    users_table = spark.sql('''
        SELECT DISTINCT
            e.userId,
            e.firstName,
            e.lastName,
            e.gender,
            e.level
        FROM event_data AS e
        WHERE e.userId IS NOT NULL
    ''')
    print("- Created users_table.")
    
    # write users table to parquet files
    users_table.write.parquet(
        output_data + "users.parquet",
        mode="overwrite"
    )
    print("- Saved parquet file for users.")

    # extract columns to create time table
    time_table = spark.sql('''
        SELECT DISTINCT
            e.ts AS start_time,
            CAST(DATE_FORMAT(FROM_UNIXTIME(e.ts/1000), 'H') as int) AS hour,
            CAST(DATE_FORMAT(FROM_UNIXTIME(e.ts/1000), 'd') as int) AS day,
            CAST(DATE_FORMAT(FROM_UNIXTIME(e.ts/1000), 'w') as int) AS week,
            CAST(DATE_FORMAT(FROM_UNIXTIME(e.ts/1000), 'M') as int) AS month,
            CAST(DATE_FORMAT(FROM_UNIXTIME(e.ts/1000), 'Y') as int) AS year,
            CAST(DATE_FORMAT(FROM_UNIXTIME(e.ts/1000), 'EEEE') as string) AS weekday,
            FROM_UNIXTIME(e.ts/1000) AS full_time
        FROM event_data AS e
        WHERE e.ts IS NOT NULL   
    ''')
    print("- Created time_table.")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(
        output_data + "time.parquet",
        mode="overwrite"
    )
    print("- Saved parquet file for time.")

    # read in song data to use for songplays table
    songs_table = spark.read.parquet(output_data + "songs.parquet")
    artists_table = spark.read.parquet(output_data + "artists.parquet")
    
    songs_table.createOrReplaceTempView("songs_data")
    artists_table.createOrReplaceTempView("artists_data")
    
    song_df = spark.sql("""
        SELECT s.song_id, s.title, a.artist_id, a.artist_name
        FROM songs_data AS s
        JOIN artists_data AS a
            ON s.artist_id = a.artist_id
        """)
    print("- Created songs_table (extended with artist columns).")
    
    song_df.createOrReplaceTempView("song_data") 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
        SELECT
            e.ts,
            e.userId,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId,
            e.location,
            e.userAgent
        FROM event_data AS e
        LEFT JOIN song_data AS s
            ON (e.artist = s.artist_name AND e.song = s.title)
        WHERE e.page = 'NextSong' AND s.title IS NOT NULL
    ''')
    print("- Created songplays_table.")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(
        output_data + "songplays.parquet",
        mode="overwrite"
    )
    print("- Saved parquet file for songplays.")


def main():
    """
    Main function to run the ETL.
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-analytics-with-spark/"
    
    process_song_data(spark, input_data, output_data)
    print("- Finished processing song_data.")
    process_log_data(spark, input_data, output_data)
    print("- Finished processing log_data.")
    print("- SUCCESSFULLY finished ETL.")


if __name__ == "__main__":
    main()
