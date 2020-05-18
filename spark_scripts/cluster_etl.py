from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = '{0}/song_data/*/*/*/*.json'.format(input_data)
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView('songs')

    # extract columns to create songs table
    songs_table = spark.sql('''
    select distinct song_id,
           title,
           artist_id,
           year,
           duration
    from songs
    ''')
    
    # write songs table to parquet files partitioned by year and artist
    output_songs_path = '{0}/songs/'.format(output_data)
    songs_table.write.parquet(output_songs_path, mode='overwrite', partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = spark.sql('''
    select distinct artist_id,
           artist_name,
           artist_location,
           artist_longitude
    from songs
    ''')    
    # write artists table to parquet files
    output_artists_path = '{0}/artists/'.format(output_data)
    artists_table.write.parquet(output_artists_path, mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = '{0}/log_data/*/*/*.json'.format(input_data)

    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView('logs')
    
    # filter by actions for song plays
    df = spark.sql('''
    select *
    from logs
    where page = 'NextSong'
    ''')

    # extract columns for users table    
    users_table = spark.sql('''
    select
        cast(e.userid as int) as user_id,
        e.firstname,
        e.lastname,
        e.gender,
        e.level
    from logs e
    join (
        select max(ts) as ts, userid
        from logs
        where page = 'NextSong'
        group by userid
    ) last_event on last_event.userid = e.userid and last_event.ts = e.ts
    ''')
    
    # write users table to parquet files
    output_users_path = '{0}/users/'.format(output_data)
    users_table.write.parquet(output_users_path, mode='overwrite')

    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', F.from_unixtime(F.col('ts')/1000))

    # create datetime column from original timestamp column
    time_table = df.select('ts', 'start_time') \
                   .withColumn('year', F.year('start_time')) \
                   .withColumn('month', F.month('start_time')) \
                   .withColumn('week', F.weekofyear('start_time')) \
                   .withColumn('weekday', F.dayofweek('start_time')) \
                   .withColumn('day', F.dayofyear('start_time')) \
                   .withColumn('hour', F.hour('start_time')).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    output_times_path = '{0}/times/'.format(output_data)
    time_table.write.parquet(output_times_path, mode='overwrite', partitionBy=['year', 'month'])  

    # read in song data to use for songplays table
    song_data = '{0}/song_data/*/*/*/*.json'.format(input_data)
    song_df = spark.read.json(song_data)
    song_df.createOrReplaceTempView('songs')

    # extract columns from joined song and log datasets to create songplays table 
    time_table.createOrReplaceTempView('times')

    songplays_table = spark.sql('''
    select distinct
        t.start_time,
        cast(e.userid as int) as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        cast(e.sessionid as int) as session_id,
        e.location as location_id,
        e.useragent as user_agent,
        t.year,
        t.month
    from logs e
    join songs s 
        on e.song = s.title 
        and e.artist = s.artist_name
    join times t
        on t.ts = e.ts
    where e.page = 'NextSong'
    ''')

    # write songplays table to parquet files partitioned by year and month
    output_songplays_path = '{0}/songplays/'.format(output_data)
    songplays_table.write.parquet(output_songplays_path, mode='overwrite', partitionBy=['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://mn-udacity-data-engineer"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
