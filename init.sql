show databases;

DROP DATABASE IF EXISTS db;

CREATE DATABASE db;

USE db;

CREATE EXTERNAL TABLE IF NOT EXISTS anime_mal(
  anime_id INT, 
  anime_url CHAR(250), 
  title CHAR(250), 
  synopsis CHAR(250), 
  main_pic CHAR(250), 
  type CHAR(250), 
  source_type CHAR(250), 
  num_episodes INT, 
  status CHAR(250), 
  start_date DATE, 
  end_date DATE, 
  season CHAR(250), 
  studios CHAR(250), 
  genres CHAR(250), 
  score DOUBLE, 
  score_count INT, 
  score_rank INT, 
  popularity_rank INT, 
  members_count INT, 
  favorites_count INT, 
  watching_count INT, 
  completed_count INT, 
  on_hold_count INT, 
  dropped_count INT, 
  plan_to_watch_count INT, 
  total_count INT, 
  score_10_count INT, 
  score_09_count INT, 
  score_08_count INT, 
  score_07_count INT, 
  score_06_count INT, 
  score_05_count INT, 
  score_04_count INT, 
  score_03_count INT, 
  score_02_count INT, 
  score_01_count INT, 
  clubs VARCHAR(250),
  pics VARCHAR(500)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'hdfs://namenode:9000/data/anime_treated/mal/';

select title from anime_mal limit 10;

CREATE EXTERNAL TABLE IF NOT EXISTS anime_2023(
  anime_id INT,
  Name CHAR(250),
  Score DOUBLE,
  Genres CHAR(250),
  Type CHAR(250),
  Episodes DOUBLE,
  Aired CHAR(250),
  Premiered CHAR(250),
  Status CHAR(250),
  Producers CHAR(250),
  Licensors CHAR(250),
  Studios CHAR(250),
  Source CHAR(250),
  Duration CHAR(250),
  Rating CHAR(250),
  Rank DOUBLE,
  Popularity INT,
  Favorites INT,
  Scored_By DOUBLE,
  Members INT,
  Image_URL CHAR(250)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'hdfs://namenode:9000/data/anime_treated/2023/';

select Name from anime_2023 limit 10;