# Namenode : 

## data files :
## - /data/mal/anime.csv
## - /data/2023/anime-dataset-2023.csv

# Leave hdfs safe mode (hdfs dfsadmin -safemode leave) - checked
docker compose exec namenode bash -c "hdfs dfsadmin -safemode leave"

## Create hdfs input directory - checked
docker compose exec namenode bash -c "hdfs dfs -mkdir -p /data/anime/mal"
docker compose exec namenode bash -c "hdfs dfs -mkdir -p /data/anime/2023"

## Copy data files to hdfs input directory - Checked
docker compose exec namenode bash -c "hdfs dfs -put /input/mal/anime.csv /data/anime/mal/anime.csv"
docker compose exec namenode bash -c "hdfs dfs -put /input/2023/anime.csv /data/anime/2023/anime.csv"

# Spark : 
## start spark with /spark.py script - checked
docker compose exec spark-master bash -c "/spark/bin/spark-submit --master 'spark://spark-master:7077' /spark.py" 

# Hive :
## Start HiveServer2 - checked
docker compose exec hive-server bash -c "hiveserver2"

## Check if HiveServer2 is up - checked
docker exec hive-server bash -c "while true; do if ! nc -z localhost 10000; then echo \"Waiting for HiveServer2 to start...\"; sleep 1; else break; fi; done"

## use beeline to connect to HiveServer2, then run /init.sql file
docker compose exec hive-server bash -c "beeline -u jdbc:hive2://localhost:10000 -f /init.sql"