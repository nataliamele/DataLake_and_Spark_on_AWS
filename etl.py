import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import udf, col, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# Creat connection with Spark
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

# Process song data to insert into DIM table
def process_song_data(spark, input_data, output_data):
    # 's3://udacity-dend/song_data'

    # read all csv files with songs data into spark data frame
    df = spark \
        .read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("charset", "UTF-8")
        .json(input_data + 'song-data/*/*/*/*.json')

    # extract columns to create songs table
    songs_table = df.select('song_id', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    # read log data file
    log_data = spark \
        .read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("charset", "UTF-8")
        .json(input_data + 'log-data/*/*/*.json')

    # filter by actions for song plays
    df = df.filter(col('page')== 'NextSong')

    # extract columns for users table
    users_table = df.select('userId','firstName', 'lastName', 'gender', 'level')

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users')

    # extract timestamp column to the new dataframe
    df_time = log_data.select('ts')

    # create datetime column from original timestamp ('ts') column
    df_time = df_time.withColumn('datetime', to_timestamp((df_time['ts']/1000) \
                     .cast('timestamp'),'yyyy-MM-dd hh:mm:ss'))


    # extract hour, day, week, month, yeat from datetime and add to the dataframe
    time_table = df_time.withColumn('hour', hour(col('datetime'))) \
                        .withColumn('day', dayofmonth(col('datetime'))) \
                        .withColumn('week', weekofyear(col('datetime'))) \
                        .withColumn('month', month(col('datetime'))) \
                        .withColumn('year', year(col('datetime')))

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time')

    # read in song data to use for songplays table
    # song_df = sqlContext.read.parquet("output_data" +'songs')
    # artists_df = sqlContext.read.parquet("output_data" +'artists')
    song_df = spark.read.json(input_data+'song-data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = song_df.join(df, song_df.artist_name == df.artist) \
                             .withColumn('songplay_id', monotonically_increasing_id()) \
                             .withColumn('start_time', to_timestamp((df['ts']/1000) \
                                                       .cast('timestamp'), 'yyyy-MM-dd hh:mm:ss')) \
                             .select('songplay_id', \
                                     'start_time', \
                                     'userId', \
                                     # col('userId'),
                                     'level', \
                                     'songId', \
                                     'artistId', \
                                     # col('sessionId'),
                                     'sessionId', \
                                     # col('artist_location'),
                                     'artist_location', \
                                     'userAgent', \
                                     month(col('start_time')).alias('month'), \
                                     year(col('start_time')).alias('year'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
