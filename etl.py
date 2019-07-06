import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    
    """
    Initate spark session.
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    Load song data from AWS S3, process them and save processed data into data model schema with parquet files.
    
    Parameters
    ----------
        spark: SparksSession
            spark session that load data and process it.
        
        input_data: str
            Input file path. Ideally from S3 buckets.
            
        output_data: str
            Output file path.
    """

    song_data = input_data + 'song_data/*/*/*/*.json'
    #song_data = input_data + 'song_data/A/B/C/TRABCEI128F424C983.json'
    
    df = spark.read.format("json").load(song_data).dropDuplicates()    
 
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    
    songs_table.write.partitionBy('year','artist_id').parquet(output_data+'songs.parquet')

    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    
    artists_table.write.partitionBy('artist_id').parquet(output_data+'artists.parquet')


def process_log_data(spark, input_data, output_data):
    
    """
    Load log data from AWS S3, process them and save processed data into data model schema with parquet files.
    
    Parameters
    ----------
        spark: SparksSession
            spark session that load data and process it.
        
        input_data: str
            Input file path. Ideally from S3 buckets.
            
        output_data: str
            Output file path.
    """
    

    log_data = input_data + 'log_data/*/*/*.json' 
    #log_data = input_data + 'log_data/2018/11/2018-11-12-events.json'
    
  
    df = spark.read.format("json").load(log_data).dropDuplicates()

    df = df.where(df.page == 'NextSong')

    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])
    
    users_table.write.partitionBy('userId').parquet(output_data+'users.parquet')

    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0),TimestampType())
    df = df.withColumn('start_time',get_timestamp(df.ts))
    
    time_table = df.select(df.start_time,
                          hour(df.start_time).alias('hour'),
                          dayofmonth(df.start_time).alias('day'),
                          weekofyear(df.start_time).alias('week'),
                          month(df.start_time).alias('month'),
                          year(df.start_time).alias('year'))
    
    time_table.write.partitionBy('year','month').parquet(output_data+'time.parquet')

    song_df = spark.read.json(input_data+'song_data/A/B/C/TRABCEI128F424C983.json')

    songplays_table = df.join(song_df,[df.artist == song_df.artist_name, df.song == song_df.title, df.length == song_df.duration],how='inner')
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
                           "userAgent",
                           month(df.start_time).alias('month'),
                           year(df.start_time).alias('year')
                          )

    songplays_table.write.partitionBy('year','month').parquet(output_data+'songsplay.parquet')


def main():
    
    """
    Main execution function.
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "/datamodel/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
