import configparser
from datetime import datetime
import os
from datetime import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import calendar
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, \
                                  date_format
from datetime import datetime as dt


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get\
                                      ("AWS", "AWS_SECRET_ACCESS_KEY")

# os.environ['AWS_ACCESS_KEY_ID']=''
# os.environ['AWS_SECRET_ACCESS_KEY']=''

song_input_data = config.get("S3", "SONG_DATA")
log_input_data = config.get("S3", "LOG_DATA")
output_data = config.get("S3", "OUTPUT_DATA")

# song_input_data = 's3a://udacity-dend/song-data/A/*/*/TRAAAAK128F9318786.json'
# log_input_data  = 's3a://udacity-dend/log-data/2018/11/2018-11-01-events.json'
# output_data = 's3a://ankit-rawat'


def create_spark_session():

    """
    The function Create a Apache Spark session to process the data

    Paramters :N/A

    Output: An Apache Spark session.
    """

    print('Creating Spark session at', dt.now())
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    print('Spark session is completed at', dt.now())
    return spark


def process_song_data(spark, input_data, output_data):
    """
    The function processes the song json files from S3 and
    creates Parquet file of song and artist.

    Paramters :

    spark : sparkSession

    input_data : input files

    output_data : output directory on S3 where parquet files
                  are to be created.

    """

    print('Processing Song data at', dt.now())

    # get filepath to song data file
    song_data = input_data

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration'].dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').\
      parquet(os.path.join(output_data,'songs.parquet'),'overwrite')

    print('Song Parquet file completed at', dt.now())
    print('Artist Parquet file started at', dt.now())

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location',\
     'artist_latitude', 'artist_longitude'].dropDuplicates()


    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,'artists.parquet'),'overwrite')
    print('Artist Parquet file completed at', dt.now())


def process_log_data(spark, input_data, output_data):

    """
    Load JSON input data from input_data path,
    process the data and write the output to AWS.

    Paramters:
    * spark         -- Spark session object
    * input_data    -- input files
    * output_data   -- output directory on S3 where parquet files
                        are to be created.

    """

    print('Processing log data at', dt.now())

    # get filepath to log data file
    log_data = input_data

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df['page']=='NextSong')


    df.printSchema()

    # extract columns for users table
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level'].dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet('users.parquet', 'overwrite')

    print('Creating time parquet file at', dt.now())


    # create timestamp column from original timestamp column
    # get_timestamp = udf(lambda a: str(int(int(a)/1000)))
    # df = df.withColumn('timestamp', get_timestamp(df.ts))

    # get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    # df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(dt.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select(
    'datetime',
    hour('datetime').alias('hour'),
    dayofmonth('datetime').alias('day'),
    weekofyear('datetime').alias('week'),
    month('datetime').alias('month'),
    year('datetime').alias('year'),
    date_format('datetime', 'F').alias('weekday'))

    time_table.show(5)
    time_table.printSchema()
    print('Writing time parquet file at', dt.now())

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').\
    parquet(os.path.join(output_data,'time.parquet'), 'overwrite')

    print('Completed time parquet file at', dt.now())

    # read in song data to use for songplays table
    song_df = spark.read.parquet("songs.parquet")
    print('Song dataframe')
    song_df.printSchema()
    song_df.select('title','artist_id').show(10)
    print('Dataframe')
    df.select('song','artist').show(10)

    # extract columns from joined song and log datasets to create songplays table
    print('Creating songplay parquet file at', dt.now())
    df = df.join(song_df, ((song_df.title == df.song)), 'inner')
    songplays_table = df['datetime', 'userId', 'level', 'song_id', \
    'artist_id', 'sessionId', 'location', 'userAgent'].dropDuplicates()

    songplays_table =  songplays_table.withColumn('month',month('datetime'))
    songplays_table =  songplays_table.withColumn('year',year('datetime'))


    songplays_table.show(5)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(\
                    os.path.join(output_data, 'songplays.parquet')\
                                , 'overwrite')
    print('Completed songplay parquet file at', dt.now())
    print('Completed ETL process at', dt.now())


def main():
    """
     Main function which creates Spark session and
     then call the process functions to load
     and process the song and log data
     Keyword arguments:
        * N/A

    """
    print('Starting ETL at', dt.now())
    spark = create_spark_session()
    # song_input_data =
    # "s3a://udacity-dend/song-data/A/*/*/TRAAAAK128F9318786.json"
    # log_input_data =
    #  "s3a://udacity-dend/log-data/*/*/*/TRAAAAK128F9318786.json"
    # output_data = ""

    # process_song_data(spark, song_input_data, output_data)
    process_log_data(spark, log_input_data, output_data)


if __name__ == "__main__":
    main()
