-- SPOTIFY'S MOST STREAMED SONGS OF 2023 (AS OF AUGUST 2023)

-- In this project I will be answering these questions:
-- Q1: Which artist has the most songs in the most streamed Spotify songs of 2023 list?
-- Q2: Which artist has the most Spotify streams on the list?
-- Q3: What's the average number of Spotify streams for these songs?
-- Q4: How many songs on the most streamed list were released in 2023?
-- Q5: Which artist has the most songs on the list where they are the sole artist?
-- Q6: Which songs on the list reached the number 1 spot on Spotify charts?
-- Q7: How many of the songs on the list were released in the last year? 5 years? 10 years? Later?
-- Q8: Does the valence percentage of a song affect its popularity?

-- I will also create a Dashboard to summarize the dataset and some of my findings. 
-- Tableau linked here: https://public.tableau.com/shared/Q5Q3FZFSC?:display_count=n&:origin=viz_share_link

-- Dataset: This dataset contains a comprehensive list of the most globally famous songs in 2023 on Spotify as of August 2023. 
-- Dataset Link: https://www.kaggle.com/datasets/nelgiriyewithana/top-spotify-songs-2023/data

-- In this analysis, I use functions such as GROUP BY, ORDER BY, LIMIT, CASE, HAVING, AVG, MIN, MAX, and more.

--------------------------------------------------------------
---- Create a database and table to import the data into: ----
--------------------------------------------------------------

CREATE DATABASE SPOTIFY;
USE SPOTIFY;
CREATE TABLE 2023_songs(track_name VARCHAR(250),
					    artist_name VARCHAR(250),
                        artist_count INT,
                        released_year YEAR,
                        released_month INT,
                        released_day INT,
                        in_spotify_playlists INT,
                        in_spotify_charts INT,
                        spotify_streams VARCHAR(250),
                        in_apple_playlists INT,
                        in_apple_charts INT,
                        in_deezer_playlists VARCHAR(10),
                        in_deezer_charts INT,
                        in_shazam_charts VARCHAR(10),
                        bpm INT,
                        song_key VARCHAR(15),
                        song_mode CHAR(5),
                        danceability DECIMAL(4,2),
                        valence DECIMAL(4,2),
                        energy DECIMAL(4,2),
                        acousticness DECIMAL(4,2),
                        instrumentalness DECIMAL(4,2),
                        liveness DECIMAL(4,2),
                        speechiness DECIMAL(4,2)
                        );
 
---------------------------------------------------- 
--------------- Data Preprocessing: ----------------
----------------------------------------------------
-- Need to clean up columns and then change their datatypes to be more convenient.

-- Delete data I won't be using:
ALTER TABLE 2023_songs DROP COLUMN in_deezer_playlists;
ALTER TABLE 2023_songs DROP COLUMN in_spotify_playlists;
ALTER TABLE 2023_songs DROP COLUMN in_apple_playlists;
ALTER TABLE 2023_songs DROP COLUMN song_key;
ALTER TABLE 2023_songs DROP COLUMN song_mode;
ALTER TABLE 2023_songs DROP COLUMN speechiness;
ALTER TABLE 2023_songs DROP COLUMN liveness;
ALTER TABLE 2023_songs DROP COLUMN instrumentalness;
ALTER TABLE 2023_songs DROP COLUMN acousticness;


-- Checking for NULL track names or artist names:
SELECT *
FROM 2023_songs
WHERE track_name IS NULL;

SELECT *
FROM 2023_songs
WHERE artist_name IS NULL;

-- There are no NULL values in these columns.


-- The max spotify_streams value is an incorrect value. We need to remove this row.

SELECT MAX(spotify_streams) as max_streams,
		MIN(spotify_streams) as min_streams
FROM 2023_songs;

DELETE FROM 2023_songs ORDER BY spotify_streams DESC LIMIT 1;

-- Change the spotify_streams column datatype from varchar to BIGINT.

ALTER TABLE 2023_songs
MODIFY spotify_streams BIGINT;


-- The column in_shazam_charts also contains commas that need to be removed.
-- The column also has empty cells which must mean that the song either is not on Shazam or is not on the charts so I will make those cells zero.

UPDATE 2023_songs SET in_shazam_charts = REPLACE(in_shazam_charts, ",", "");

SELECT in_shazam_charts
FROM 2023_songs
WHERE in_shazam_charts IS NULL;

UPDATE 2023_songs SET in_shazam_charts = 0 WHERE in_shazam_charts = "";

-- Change the in_shazam_charts column datatype to INT.
ALTER TABLE 2023_songs
MODIFY in_shazam_charts INT;


-- Check if any other columns have empty cells. 
SELECT bpm
FROM 2023_songs
WHERE bpm = "";

SELECT danceability
FROM 2023_songs
WHERE danceability = "";

SELECT valence
FROM 2023_songs
WHERE valence = "";

SELECT energy
FROM 2023_songs
WHERE energy = "";

-- none of these columns have missing values.


-- Check for outliers in the released_year.
SELECT track_name,
		released_year
FROM 2023_songs
ORDER BY released_year ASC
LIMIT 10;

SELECT track_name,
		released_year
FROM 2023_songs
ORDER BY released_year DESC
LIMIT 10;

-- One song, "Agudo Magico 3", has a release year of 1930 but upon inspection, I found that it was actually released in 2022.
UPDATE 2023_songs SET released_year = 2022 WHERE track_name = "Agudo Magico 3";

SELECT released_year
FROM 2023_songs 
WHERE track_name = "Agudo Magico 3";


-- Check that released_month ranges from 1-12 and released_day ranges from 1-31.
SELECT MIN(released_month) AS min_month,
		MAX(released_month) AS max_month
FROM 2023_songs;

SELECT MIN(released_day) AS min_day,
		MAX(released_day) AS max_day
FROM 2023_songs;


-- Check for and remove duplicate song titles from the same artist.
SELECT *
FROM 2023_songs
WHERE track_name IN (SELECT track_name
                            FROM 2023_songs
                            GROUP BY track_name
                            HAVING COUNT(track_name) > 1);

-- Some duplicate tracks are from different artists, so I will leave those alone. 
-- Some are tracks from the same artist that were entered twice, so I will remove the row incorrect or less data (ones with multiple zero entries).

DELETE FROM 2023_songs WHERE track_name = "SNAP" AND in_spotify_charts = 0;

DELETE FROM 2023_songs WHERE track_name = "About Damn Time" AND released_month = 7;

DELETE FROM 2023_songs WHERE track_name = "SPIT IN MY FACE!" AND in_apple_charts = 0;

DELETE FROM 2023_songs WHERE track_name = "Take My Breath" AND in_apple_charts = 73;



----------------------------------------------------
----------------- Data Analysis: -------------------
----------------------------------------------------

-- Q1: Which artist has the most songs in the most streamed Spotify songs of 2023 list?

-- First, I need to separate the artist names into different columns for songs that have multiple artists. 
-- This was challenging because the number of artists for each song varies.
-- I decided to create a temporary table to split the artist names into columns.
-- When I go to calculate the number of songs an artist has, I will count a row if ANY of the columns contain their name.

CREATE TEMPORARY TABLE artist_names(row_num INT,
									artist_1 VARCHAR(50),
									artist_2 VARCHAR(50),
                                    artist_3 VARCHAR(50),
                                    artist_4 VARCHAR(50),
                                    artist_5 VARCHAR(50),
                                    artist_6 VARCHAR(50),
                                    artist_7 VARCHAR(50),
                                    artist_8 VARCHAR(50),
                                    streams BIGINT
                                    );

-- This is used to create row numbers.
SET @row_number = 0;

-- Inserting the column calculations.
INSERT INTO artist_names(row_num, artist_1, artist_2, artist_3, artist_4, artist_5, artist_6, artist_7, artist_8, streams)
SELECT (@row_number:= @row_number + 1),
		SUBSTRING_INDEX(artist_name, ",", 1),
		IF(SUBSTRING_INDEX(artist_name, ",", 1) = SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -1), ",", 1), NULL, SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -1), ",", 1)),
        IF(SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -1), ",", 1) = SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -2), ",", 1), NULL, SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -2), ",", 1)),
        IF(SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -2), ",", 1) = SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -3), ",", 1), NULL, SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -3), ",", 1)),
        IF(SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -3), ",", 1) = SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -4), ",", 1), NULL, SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -4), ",", 1)),
        IF(SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -4), ",", 1) = SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -5), ",", 1), NULL, SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -5), ",", 1)),
        IF(SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -5), ",", 1) = SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -6), ",", 1), NULL, SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -6), ",", 1)),
        IF(SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -6), ",", 1) = SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -7), ",", 1), NULL, SUBSTRING_INDEX(SUBSTRING_INDEX(artist_name, ",", -7), ",", 1)),
        spotify_streams
FROM 2023_songs;

-- View the temporary table.
SELECT * FROM artist_names;

-- Count the number of songs each artist has on the most streamed list and find the artist with the most songs by sorting in descending order.
SELECT artist_1 AS artist,
		COUNT(row_num) AS number_of_songs
FROM artist_names
WHERE artist_1 IN (artist_1, artist_2, artist_3, artist_4, artist_5, artist_6, artist_7, artist_8)
GROUP BY artist_1
ORDER BY number_of_songs DESC;

-- Taylor Swift has the most songs on the list with 36 songs. Next is The Weeknd with 33 songs.

-----------------------------------------------------------------------------------------------------------------------------------

-- Q2: Which artist has the most Spotify streams on the list?
SELECT artist_1 AS artist,
		SUM(streams) AS total_spotify_streams
FROM artist_names
WHERE artist_1 IN (artist_1, artist_2, artist_3, artist_4, artist_5, artist_6, artist_7, artist_8)
GROUP BY artist_1
ORDER BY total_spotify_streams DESC;

-- The Weeknd has the most Spotify streams, followed by Bad Bunny, Ed Sheeran, and then Taylor Swift. 
-- This is interesting because Taylor Swift has the most songs on the list, but the 5th most streams.

-----------------------------------------------------------------------------------------------------------------------------------

-- Q3: What's the average number of Spotify streams for these songs?
SELECT AVG(spotify_streams) AS avg_spotify_streams
FROM 2023_songs
WHERE released_year = 2023;

-- The average Spotify streams for songs released in 2023 is 147,477,052, whereas the average Spotify streams for all songs on the list is 514,017,927. 

-----------------------------------------------------------------------------------------------------------------------------------

-- Q4: How many songs on the most streamed list were released in 2023?
SELECT COUNT(track_name) AS num_songs
FROM 2023_songs
WHERE released_year = 2023;

SELECT COUNT(track_name) AS num_songs
FROM 2023_songs
WHERE released_year != 2023;

-- 948 songs are on the Spotify most streamed songs in 2023 list.
-- 175 of those songs were released in 2023.
-- 773 of those songs were released in other years.

-----------------------------------------------------------------------------------------------------------------------------------

-- Q5: Which artist has the most songs on the list where they are the sole artist?
SELECT artist_1 AS artist,
		COUNT(row_num) AS number_of_songs
FROM artist_names
WHERE artist_2 IS NULL
GROUP BY artist
ORDER BY number_of_songs DESC;

-- 301 of the 948 songs feature only 1 artist.
-- Taylor Swift was at the top of this list having 34 songs where she was the sole artist. This means that only 2 of her songs on this list featured other artists.
-- Next is The Weeknd with 21 songs which means 12 of his songs featured other artists.
-- It would be interesting to compare the number of songs by an artist to how many of those songs have no feature.

-----------------------------------------------------------------------------------------------------------------------------------

-- Q6: Which songs on the list reached the number 1 spot on Spotify charts?
SELECT *
FROM 2023_songs
WHERE in_spotify_charts = 2;

-- There were 16 songs that have reached the number 1 spot on Spotify charts.
-- Only 2 artists have had multiple songs in the number 1 spot. 5 of the 16 are by Harry Styles and 2 of the 16 feature Bad Bunny.

-----------------------------------------------------------------------------------------------------------------------------------

-- Q7: How many of the songs on the list were released in the last year? 5 years? 10 years? Later?
SELECT CASE 
		WHEN released_year <= 2012 THEN "songs_older_than_10_years"
        WHEN released_year <= 2017 AND released_year >= 2013 THEN "songs_6to10_years_old"
        WHEN released_year <= 2022 AND released_year >= 2018 THEN "songs_1to5_years_old"
        WHEN released_year = 2023 THEN "songs_from_2023"
        END AS song_age,
		COUNT(track_name)/948 AS num_songs
FROM 2023_songs
GROUP BY song_age;
        
-- 18.46% of songs were released in 2023.
-- 63.39% of songs are 1-5 years old.
-- 8.23% of songs are 6-10 years old.
-- 9.92% of songs are older than 10 year.

-----------------------------------------------------------------------------------------------------------------------------------

-- Q8: Does the valence percentage of a song affect its popularity?
SELECT CASE
			WHEN valence <= 25 THEN "0-25%"
			WHEN valence > 25 AND valence <= 50 THEN "25-50%"
			WHEN valence > 50 AND valence <= 75 THEN "50-75%"
			ELSE "75-100%"
		END AS valence_levels,
        AVG(spotify_streams) AS avg_streams
FROM 2023_songs
GROUP BY valence_levels
ORDER BY avg_streams DESC;

-- There is a clear correlation between the valence level of a song and its popularity. 
-- Songs with valence percentage between 0-25% are the most popular. Followed by songs with 25-50%, 50-75%, and 75-100% respectively.
-- High valence percentages means a song sounds more popular and low valence means it sounds more negative. So, negative sounding songs are more popular.
