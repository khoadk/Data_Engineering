import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS log_dataset;"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_dataset;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS log_dataset 
(
    artist TEXT,
    auth TEXT,
    firstname TEXT,
    gender CHAR(1),
    iteminsession INTEGER,
    lastname TEXT,
    length DECIMAL,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration DECIMAL,
    sessionid TEXT,
    song TEXT,
    status TEXT,
    ts BIGINT,
    useragent TEXT,
    userid TEXT
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS song_dataset
(
    artist_id TEXT,
    artist_latitude DECIMAL,
    artist_location TEXT,
    artist_longitude DECIMAL,
    artist_name TEXT,
    duration TEXT,
    num_songs INTEGER,
    song_id TEXT,
    title TEXT,
    year TEXT
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays 
(
    songplay_id BIGINT IDENTITY(0,1), 
    start_time TIMESTAMP, 
    user_id TEXT, 
    level TEXT, 
    song_id TEXT, 
    artist_id TEXT, 
    session_id TEXT, 
    location TEXT, 
    user_agent TEXT,
    PRIMARY KEY (songplay_id) 
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
(
    user_id TEXT,
    first_name TEXT,
    last_name TEXT,
    gender CHAR(1),
    level TEXT,
    PRIMARY KEY (user_id)
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT, 
    title TEXT, 
    artist_id TEXT, 
    year TEXT, 
    duration DECIMAL, 
    PRIMARY KEY (song_id) 
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT, 
    name TEXT, 
    location TEXT, 
    latitude DECIMAL, 
    longitude DECIMAL, 
    PRIMARY KEY (artist_id) 
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP, 
    hour TEXT, 
    day TEXT, -- Day of month
    week TEXT, -- Week of year
    month TEXT, 
    year TEXT, 
    weekday TEXT, 
    PRIMARY KEY (start_time) 
);
""")

# STAGING TABLES

staging_events_copy = ("""copy log_dataset 
                          from 's3://udacity-dend/log_data/2018/11/2018-11-01-events.json'
                          iam_role {}
                          json {}
                          region 'us-west-2';
                       """).format(IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""copy song_dataset 
                          from 's3://udacity-dend/song_data/A/A/A'
                          iam_role {}
                          json 'auto'
                          region 'us-west-2';
                      """).format(IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""BEGIN;

INSERT into songplays( 
    start_time, user_id, level,
    song_id, artist_id,
    session_id, location, user_agent)
SELECT timestamp 'epoch' + log_dataset.ts/1000 * interval '1 second' as start_time, 
log_dataset.userid, log_dataset.level,
song_dataset.song_id, song_dataset.artist_id,
log_dataset.sessionId, log_dataset.location, log_dataset.userAgent
FROM log_dataset LEFT JOIN song_dataset
ON LOWER(log_dataset.artist) = LOWER(song_dataset.artist_name)
AND LOWER(log_dataset.song) =  LOWER(song_dataset.title)
AND ROUND(CONVERT(REAL,log_dataset.length)) =  ROUND(CONVERT(REAL,song_dataset.duration))
WHERE log_dataset.page = 'NextSong';

COMMIT;
""")

user_table_insert = ("""BEGIN;

INSERT into users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userid, firstName, lastName, gender, level
FROM log_dataset
WHERE log_dataset.page = 'NextSong' 
AND userid IS NOT NULL;

COMMIT;
""")

song_table_insert = ("""BEGIN;

INSERT into songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT song_id, title, artist_id, convert(integer, year), convert(decimal, duration)
FROM song_dataset
WHERE song_id IS NOT NULL;

COMMIT;
""")

artist_table_insert = ("""BEGIN;

INSERT into artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT artist_id, artist_name, artist_location, convert(decimal, artist_latitude), convert(decimal, artist_longitude)
FROM song_dataset
WHERE artist_id IS NOT NULL;


COMMIT;
""")

time_table_insert = ("""BEGIN;


INSERT into time (start_time, hour, day, week, month, year, weekday) 
SELECT DISTINCT start_time,
to_char (start_time, 'HH24'),
to_char (start_time, 'DD'),
to_char (start_time, 'WW'),
to_char (start_time, 'MM'),
to_char (start_time, 'YYYY'),
to_char (start_time, 'Day')
FROM songplays
WHERE start_time IS NOT NULL;


COMMIT;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
