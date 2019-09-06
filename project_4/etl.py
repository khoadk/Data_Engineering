import configparser
from datetime import datetime
import os
#import glob
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


# def remane col names

def rename_cols(schema, old_columns, new_columns):
    """
    Description:
        Rename column names. It is helpfull way to keep track data mapping. 
        Which columns were mapped to which columns of "schema on read". 
    
    Parameters:
        - schema: dataframe whose column names are exptected to renamed.
        - old_names: the current column names of df.
        - new_names: the expected column new names of df
    """
    try:
        print("rename_cols fucntion is starting")
        print("********************************")
        for old_col,new_col in zip(old_columns,new_columns):
            schema = schema.withColumnRenamed(old_col,new_col)
        print("Successful renaming.")
        print("*********************")
        return schema
    except:
        print("Unsuccessful renaming.")
        print("**********************")

# def the schema on read for song dataset

def schema_song_data():
    """
    Description:
        Schema design for song datasets.
    """
    try:
        print("schema_song_data fuction is statrting.")
        print("**************************************")
        
        schema = R([
            Fld("artist_id",Str()),
            Fld("artist_latitude",Dbl()),
            Fld("artist_location",Str()),
            Fld("artist_longitude",Dbl()),
            Fld("artist_name",Str()),
            Fld("duration",Dbl()),
            Fld("num_songs",Int()),
            Fld("song_id",Str()),
            Fld("title",Str()),
            Fld("year",Int()),
        ])
        
        print("schema_song_data is successfull created")
        print("***************************************")
        return schema
    
    except:
        print("schema_song_data function is successful created.")
        print("************************************************")
        
# def schema on read for log dataset

def schema_log_data():
    """
    Description:
        schema design for log dataset.
    """
    try:
        print("schema_log_data function is starting.")
        print("*************************************")
        
        schema = R([
            Fld("artist",Str()),
            Fld("auth",Str()),
            Fld("firstName",Str()),
            Fld("gender",Str()),
            Fld("itemInSession",Int()),
            Fld("lastName",Str()),
            Fld("length",Dbl()),
            Fld("level",Str()),
            Fld("location",Str()),
            Fld("method",Str()),
            Fld("page",Str()),
            Fld("registration",Int()),
            Fld("sessionId",Int()),
            Fld("song",Str()),
            Fld("status",Str()),
            Fld("ts",Int()), 
            Fld("userAgent",Str()),
            Fld("userId",Int())
        ])
        return schema
        
        print("Successfull schema design for log dataset.")
        print("******************************************")
             
    except:
        print("Unsuccessful schema design for log dataset.")
        print("*******************************************")

# def "process_song_data" elt for song datasets.

def process_song_data(spark, input_data, output_data):
    """
    Description:
        ELT for song dataset: 
        - Extract data from S3 
        - Load in into Schema on read 
        - Transform then save it back to S3 in parquet file format. 
        
    Parameters:
        - spark: spark Session
        - input_data: directory for song dataset
        - output_data: directory for tranformed dataset.
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    
    # read song data file
    song_dataset_schema = schema_song_data()
    df = spark.read.json(song_data, schema = song_dataset_schema)

    # extract columns to create songs table
    song_cols = ["song_id", "title", 
                     "artist_id", "year", 
                     "duration"]
    song_table = df.select(song_cols).dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    song_table.write.partitionBy("year","artist_id") \
                .mode("overwrite") \
                .parquet(output_data + 'song_table/')
    print("Successful write songs table to parquet files partitioned by year and artist")
    print("*****************************************************************************")

    # extract columns to create artists table
    artists_cols = ["artist_id", "artist_name", 
                     "artist_location", "artist_latitude", 
                     "artist_longitude"]
    artists_table = df.select(artists_cols).dropDuplicates()
          
    # Remane the columns name of artist table matching shema design
    rename = ["artist_id","name","location","latitude","longitude"]
    artists_table = rename_cols(artists_table, artists_table.columns, rename)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite") \
        .parquet(output_data + 'artist_table/') 
    print("Successful write artists table to parquet files")
    print("***********************************************")
    print("******************process_song_data si DONE******************")


# def "process_log_data" elt for log datasets.

def process_log_data(spark, input_data, output_data):
    """
    Description:
        ELT for log dataset: 
        - Extract data from S3 
        - Load in into Schema on read 
        - Transform then save it back to S3 in parquet file format. 
        
    Parameters:
        - spark: spark Session
        - input_data: directory for log dataset
        - output_data: directory for tranformed dataset.
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    #log_data = input_data + 'log_data/*.json'
    

    # read log data file
    #log_dataset_schema = schema_log_data()
    df = spark.read.json(log_data, schema = log_dataset_schema)
    #df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    
    # filter by actions for user table
    user_cols = ["userId","firstName","lastName","gender","level"]
    user_table = df.select(user_cols).dropDuplicates()
    
    # Remane the columns name of user table matching shema design
    rename = ["user_id","first_name","last_name","gender", "level"]
    user_table = rename_cols(user_table, user_table.columns, rename)
    
    # write users table to parquet files
    user_table.write.mode("overwrite") \
        .parquet(output_data + "user_table/")
    print("Successful write users table to parquet files")
    print("*********************************************")
    
    # create timestamp column from original timestamp column
    
    df = df.withColumn("start_time", F.to_timestamp(df.ts/1000) )
    
    
    # create datetime column from original timestamp column
    time_table = df.select("start_time").dropDuplicates() \
                    .withColumn("hour", hour(col("start_time"))) \
                    .withColumn("day", dayofmonth(col("start_time"))) \
                    .withColumn("week", weekofyear(col("start_time"))) \
                    .withColumn("month", month(col("start_time"))) \
                    .withColumn("year", year(col("start_time"))) \
                    .withColumn("weekday", date_format(col("start_time"), 'E'))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month") \
        .mode("overwrite") \
        .parquet(output_data + "time_table/")
    print("Successful write time table to parquet files")
    print("********************************************")
    
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "song_table/")
    song_col = ["song_id", "title", "artist_id"]
    song_df = song_df.select(song_col)
    
    # extract columns from joined song and log datasets to create songplays table 
    # Denormalize schema => ADDING song, year and month to improve queries performance.
    
    songplays_cols = ["ts","userId", "level", "sessionId","location","userAgent","song"]
    songplays_table = df.select(songplays_cols).dropDuplicates()
    
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    
    songplays_table = songplays_table.withColumn("start_time", F.to_timestamp(songplays_table.ts/1000) )
    
    songplays_table = songplays_table.withColumn("month", F.month(songplays_table.start_time)) \
                                    .withColumn("year", F.year(songplays_table.start_time))
    
    
    songplays_table = songplays_table.join(song_df, song_df.title == songplays_table.song, how = "left")
                                       
    songplays_table = songplays_table.drop(songplays_table.title)
  
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month") \
        .mode("overwrite") \
        .parquet(output_data + "songplays_table/")
    print("Successful write songplays_table to parquet files")
    print("*************************************************")
    print("******************DONE***************************")
    
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://udacity-khoa-nguyen/test_output/"
    #output_data = "s3://udacity-khoa-nguyen/udacity-data-lake/"

    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
