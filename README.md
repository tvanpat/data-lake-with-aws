# Data Lake with Spark
 Udacity Data Lake Project for Data Engineering Nanodegree

 This README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository.

 ## Getting Started

 1.  You will need to first connect to AWS  First open the dlf.cfg file and enter your aws key and secret.  YOU MUST ADD THIS FILE TO YOUR .gitignore if you are using github.  Failure to do so may expose your aws key and secret.

 2.  Run the create_table.py using the following command:
 > python etl.py


 ## Prerequisites
 1.  **pyspark**


 ## Purpose
 The purpose of this database is to conduct ETL operations and store data from user activity from the Sparkify app.  
 This data will be used by the Sparkify analytics team will use this data gain a greater understanding of user activity and songs being listened to.


 ## File Description

 - dl.cfg
  - This file is used to store the AWS Key and AWS Secret.  Add to your .gitignore file to ensure your key and secret are not exposed.

- etl.py
 - This python script is used to retrieve song and log data from s3 bucket, transform the data and then load parqest files back into s3.

- sql_queries.py
 - This python script contains the sql queries used to create tables and transform/insert data into the tables.

## Data Sets
There are two dataets located in s3 Buckets on AWS.  
- Song Data:  This is located at s3://udacity-dend/song_data.  A sample of the data is below:
 >{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}


 - Log Data:  This is located at s3://udacity-dend/log_data.  A sample of the data is below:
  > {"artist":"Pavement", "auth":"Logged In", "firstName":"Sylvie", "gender", "F", "itemInSession":0, "lastName":"Cruz", "length":99.16036, "level":"free", "location":"Klamath Falls, OR", "method":"PUT", "page":"NextSong", "registration":"1.541078e+12", "sessionId":345, "song":"Mercy:The Laundromat", "status":200, "ts":1541990258796, "userAgent":"Mozilla/5.0(Macintosh; Intel Mac OS X 10_9_4...)", "userId":10}


 ## Database Schema
 There are 5 tables in the database.  This design focuses on the songplay table which houses the most important information for the analytics team.  The fact tables of time, users, songs, and artists help to provide context and additional details for the dimension songplay table.

The use tables are the **songplay_fact**, **time_dim**, **user_dim**, **song_dim**, and **artist_dim** tables.  These tables are in the

 The **time table** which contains:

 | Field        | Data Type          |
  |-------------  | ------------- |
 | start_time      |int |
 | hour      | int     |
 | day | int      |
 | week | int     |
 | month | int      |
 | year | int     |  
 | weekday | int     |

 The **users table** which contains:

 | Field        | Data Type          |
 | ------------- | ------------- |
 | user_id      | int |
 | first_name      | varchar      |
 | last_name | varchar      |
 | gender | varchar      |
 | level | varchar     |

 The **songs table** which contains:

 | Field        | Data Type          |
 | ------------- | ------------- |  
 | song_id      | varchar |
 | title      | varchar      |
 | artist_id | varchar      |
 | artist_name | varchar      | 
 | year | int     |
 | duration | float     |

 The **artists table** which contains:

 | Field        | Data Type          |
 | ------------- | ------------- |  
 | artist_id      | varchar |
 | name      | varchar      |
 | location | varchar      |  
 | latitude | varchar      |
 | longitude | varchar   |

 The **songplay table** which contains:

 | Field        | Data Type          |
 | ------------- | ------------- |
 | songplay_id      | int |
 | start_time      | int    |  
 | user_id | int  |  
 | song_id | varchar      |  
 | artist_id | varchar     |  
 | session_id | int  |
 | location | varchar      |
 | user_agent | varchar    |

 ![ERD Diagram](./images/snowflake_erd.PNG)
