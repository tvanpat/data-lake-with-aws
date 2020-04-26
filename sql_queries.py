# Insert data into song_data_table
song_table_query = """SELECT DISTINCT stb.song_id,
                                            stb.title,
                                            stb.artist_id,
                                            stb.year,
                                            stb.duration
                                            FROM song_table stb
                                            WHERE stb.song_id IS NOT NULL
                                        """

# Insert data into the artists_table
artists_table_query = """SELECT DISTINCT stb.artist_id,
                                        stb.artist_name,
                                        stb.artist_location,
                                        stb.artist_latitude,
                                        stb.artist_longitude
                                        FROM song_table stb
                                        WHERE stb.artist_id IS NOT NULL
                                    """
# Insert data into the user_table
user_query = """SELECT DISTINCT ut.userId as user_id,
                            ut.firstName as first_name,
                            ut.lastName as last_name,
                            ut.gender as gender,
                            ut.level as level
                            FROM log_table ut
                            WHERE ut.userId IS NOT NULL
                        """

# This query will read in the timestamp column from the dataframe and convert it to timestamp in SQL
time_query = """SELECT tms.start_time_sub as start_time,
                            hour(tms.start_time_sub) as hour,
                            dayofmonth(tms.start_time_sub) as day,
                            weekofyear(tms.start_time_sub) as week,
                            month(tms.start_time_sub) as month,
                            year(tms.start_time_sub) as year,
                            dayofweek(tms.start_time_sub) as weekday
                            FROM
                            (SELECT to_timestamp(time_st.ts/1000) as start_time_sub
                            FROM log_table time_st
                            WHERE time_st.ts IS NOT NULL
                            ) tms
                        """
# Insert data into the songplay_table
songplay_query = """SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(lt.ts/1000) as start_time,
                                month(to_timestamp(lt.ts/1000)) as month,
                                year(to_timestamp(lt.ts/1000)) as year,
                                lt.userId as user_id,
                                lt.level as level,
                                st.song_id as song_id,
                                st.artist_id as artist_id,
                                lt.sessionId as session_id,
                                lt.location as location,
                                lt.userAgent as user_agent
                                FROM log_table lt
                                JOIN song_table st on lt.artist = st.artist_name and lt.song = st.title
                            """
