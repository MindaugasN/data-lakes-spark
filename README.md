# Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Datasets

>s3://udacity-dend/song_data
s3://udacity-dend/log_dat

#### song_data preview

The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

>song_data/A/A/B/TRAABJL12903CDCF1A.json

```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

#### log_data preview

The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings. The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

>log_data/2018/11/2018-11-01-events.json

```json
{"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}
```

## Tasks

Using the song and event datasets, we'll need to create set of dimensional tables in S3.

**Fact Table**
1. songplays - records in event data associated with song plays i.e. records with page `NextSong`
    * *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

**Dimension Tables**
1. **users** - users in the app
    * *user_id, first_name, last_name, gender, level*
2. **songs** - songs in music database
    * *song_id, title, artist_id, year, duration*
3. **artists** - artists in music database
    * *artist_id, name, location, lattitude, longitude*
4. **time** - timestamps of records in songplays broken down into specific units
    * *start_time, hour, day, week, month, year, weekday*

# Solution

The main task is building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Project structure

```
project
|-- local_data                               # ---> Locally downloaded data for testing
    |-- log-data.zip
    |-- song-data.zip
|-- README.md
|-- etl.py                                   # ---> Main script for processing all data from S3 and storing again to S3
|-- dl.cfg.dist                              # ---> Template config for AWS credentials
|-- .gitignore
```

## Requirements

1. Software:

- [Python 3.6.x](http://docs.python-guide.org/en/latest/starting/installation/)
- [pip](https://pip.pypa.io/en/stable/installing/)

2. Python 3rd party libraries:

3. AWS account:

- [AWS credentials](https://aws.amazon.com/getting-started/)

## How to use this repo?

1. Clone or download it.
2. Make sure you met all the requirements listed above.
3. Create a file `dl.cfg` from `dl.cfg.dist` and fill your AWS credentials.