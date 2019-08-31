# Author: khoa.nguyen.acb@gmail.com
# Project: Data Lake
Building a data lake and an ETL pipeline in Spark that loads data from S3, processes the data in analystics tables, and load them back into S3.

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

## Project Datasets
You'll be working with two datasets that reside in S3. Here are the S3 links for each:
> Song data: s3://udacity-dend/song_data.
>
> Log data: s3://udacity-dend/log_data.

### Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
> song_data/A/B/C/TRABCEI128F424C983.json.
>
> song_data/A/A/B/TRAABJL12903CDCF1A.json.

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
> {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}.

## Schema for Song Play Analysis
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
1. **songplays** - records in log data associated with song plays i.e. records with page #### NextSong.
    * *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*.












