# TABLES
table_names = ["songplays", "users", "songs", "artists", "time" ]

# DROP TABLES
songplays_table_drop = "DROP TABLE IF EXISTS songplays"
users_table_drop = "DROP TABLE IF EXISTS users"
songs_table_drop = "DROP TABLE IF EXISTS songs"
artists_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

## FACT TABLE - songplay -> records in log data associated with song plays i.e. records with page NextSong
songplays_table_create = ("""
                         CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY, 
                                                               start_time timestamp NOT NULL,
                                                               user_id int NOT NULL, 
                                                               level varchar, 
                                                               song_id varchar, 
                                                               artist_id varchar, 
                                                               session_id int, 
                                                               location varchar, 
                                                               user_agent varchar)

                         """)

## DIMENSION TABLES

### users - users in the app
users_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, 
                                                          first_name varchar, 
                                                          last_name varchar, 
                                                          gender varchar, 
                                                          level varchar)
""")

### songs - songs in music database
songs_table_create = ("""
                    CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, 
                                                      title varchar, 
                                                      artist_id varchar, 
                                                      year int, 
                                                      duration float)
                     """)

### artists - artists in music database
artists_table_create = (""" 
                       CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, 
                                                           name varchar, 
                                                           location varchar, 
                                                           latitude float, 
                                                           longitude float)
                       """)

### time - timestamps of records in songplays broken down into specific units
time_table_create = (""" CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, 
                                                          hour int, 
                                                          day int, 
                                                          week int, 
                                                          month int, 
                                                          year int, 
                                                          weekday int)
""")

# INSERT RECORDS

songplays_table_insert = ("""
                          INSERT INTO songplays(start_time, user_id, level, song_id, 
                                                artist_id , session_id , location , user_agent) 
                                         VALUES(%s, %s, %s, %s, %s, %s, %s, %s )
                          ON CONFLICT DO NOTHING
                          """)
                    

users_table_insert = ("""
                      INSERT INTO users(user_id, first_name, last_name, gender, level)
                                  VALUES(%s, %s, %s, %s, %s)
                      ON CONFLICT DO NOTHING
                      """)

songs_table_insert = ("""
                      INSERT INTO songs("song_id", "title", "artist_id", "year", "duration")
                                 VALUES(%s, %s, %s, %s, %s)
                      ON CONFLICT DO NOTHING
                      """)

artists_table_insert = ("""
                        INSERT INTO artists(artist_id, name, location, latitude, longitude)
                                      VALUES(%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """)

time_table_insert = ("""
                     INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                                 VALUES(%s, %s, %s, %s, %s, %s, %s)
                     ON CONFLICT DO NOTHING
                     """)

# FIND SONGS
## to find the song ID and artist ID based on the title, artist name, and duration of a song.
song_select = ("""
               SELECT songs.song_id, artists.artist_id, songs.duration
               FROM songs
               JOIN artists ON songs.artist_id = artists.artist_id
               WHERE songs.title=%s 
                     AND 
                     artists.name=%s 
                     AND 
                     songs.duration=%s
               """)

# QUERY LISTS
create_table_queries = [songplays_table_create, users_table_create, songs_table_create, artists_table_create, time_table_create]
drop_table_queries = [songplays_table_drop, users_table_drop, songs_table_drop, artists_table_drop, time_table_drop]