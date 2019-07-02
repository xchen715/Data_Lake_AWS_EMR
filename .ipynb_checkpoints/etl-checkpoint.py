import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, SttuctField. StringType, FloatType, TimestampType, IntegerType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    song_struct = StructType([StructField("num_songs", IntegerType(), True),
                         StructField("artist_id", StringType(), True),
                         StructField("artist_latitude", FloatType(), True),
                         StructField("artist_longitude", FloatType(), True),
                         StructField("artist_location", StringType(), True),
                         StructField("artist_name", StringType(), True),
                         StructField("song_id", StringType(), True),
                         StructField("title", StringType(), True),
                         StructField("duration", FloatType(), True),
                         StructField("year"), IntegerType(), True])

    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json.(song_data,schema=song_struct)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(output_data+'songs.pq')

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    
    # write artists table to parquet files
    artists_table.write.partitionBy('artist_id').parquet(output_data+'artists.pq')


def process_log_data(spark, input_data, output_data):
    
    log_struct = StructType([StructField("artist", StringType(), True),
                         StructField("auth", StringType(), True),
                         StructField("firstName", StringType(), True),
                         StructField("gender", StringType(), True),
                         StructField("itemInSession", IntegerType(), True),
                         StructField("lastName", StringType(), True),
                         StructField("length", FloatType(), True),
                         StructField("level", StringType(), True),
                         StructField("location", StringType(), True),
                         StructField("method", StringType(), True),
                         StructField("page", StringType(), True),
                         StructField("registration", FloatType(), True),
                         StructField("sessionId", IntegerType(), True),
                         StructField("song", StringType(), True),
                         StructField("status", IntegerType(), True),
                         StructField("ts", TimestampType(), True),
                         StructField("userAgent", StringType(), True),
                         StructField("userId", StringType(), True)])

    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json' # log_data/2018/11/2018-11-12-events.json

    # read log data file
    df = spark.read.json.(log_data,schemalog_struct)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    user_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])
    
    # write users table to parquet files
    artists_table.write.partitionBy('userId').parquet('users.pq')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0),TimestampType())
    df = df.withColumn('start_time',get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.select(df.start_time,
                          hour(df.start_time).alias('hour'),
                          dayofmonth(df.start_time).alias('day'),
                          weekofyear(df.start_time).alias('week'),
                          month(df.start_time).alias('month'),
                          year(df.start_time).alias('year'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data+'time.pq')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,[df.artist == song_df.artist_name, df.song == song_df.title, df.length = song_df.duration],how='inner')
    songplays_table = songplays_table.withColumn('songplay_id',monotonically_increasing_id())
    songplays_table = songplays_table.withColumn('start_time',get_timestamp(songplays_table.ts))
    songplays_table = songplays_table.select('songplay_id',
                           'start_time',
                           songplays_table.userId.alias('user_Id'),
                           'level',
                           'song_Id',
                           'artist_id',
                           songplays_table.sessionId.alias('session_id'),
                           songplays_table.artist_location.alias('location'),
                           "userAgent"
                          )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(output_data+'songsplay.pq')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
