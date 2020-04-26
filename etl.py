import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from sql_queries import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    '''
    This function creates the Sprk session.  This is required to interact with Spark.
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function reads the song json files from the s3 bucket, processes the files with Spark and then saves the results in a different s3 Bucket.
    '''
    # get filepath to song data file
    # TODO: Fix Song_data_ Path
    song_data_path = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data_path)

    # create a spark view to enable the use of sql style commands
    songs_table = df.createOrReplaceTempView("song_table")

    # extract columns to create a table for song data
    songs_table = spark.sql(song_table_query)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy(
        "year", "artist_id").parquet(output_data+'song_table/')

    # extract columns to create artists table
    artists_table = spark.sql(artists_table_query)

    # write artists table to parquet files
    artists_table = artists_table.write.mode('overwrite').parquet(output_data+'artist_table/')


def process_log_data(spark, input_data, output_data):
    '''
    This function reads the log json files from the s3 bucket, processes the files with Spark and then saves the results in a different s3 Bucket.
    '''
    # get filepath to log data file
    log_path = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_path)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # create a spark view to enable the use of sql style commands
    df.createOrReplaceTempView("log_table")

    # extract columns for users table
    user_table = spark.sql(user_query)

    # write users table to parquet files
    user_table.write.mode('overwrite').parquet(output_data+'user_table/')

    # extract columns to create time table
    time_table = spark.sql(time_query)

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy(
        "year", "month").parquet(output_data+'time_table/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/song_table/")
    song_df.createOrReplaceTempView("song_table")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql(songplay_query)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').parquet(output_data+'sonplay_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://vp-aws-udacity-data-lake-output/"

    #process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
