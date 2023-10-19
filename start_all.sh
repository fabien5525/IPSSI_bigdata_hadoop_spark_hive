## data files :
## - /data/mal/anime.csv
## - /data/2023/anime-dataset-2023.csv

echo "[SCRIPT] Setup started"
echo ""

# Leave hdfs safe mode (hdfs dfsadmin -safemode leave) - checked
echo ""
echo "[SCRIPT] Leaving hdfs safe mode"
echo ""
docker compose exec namenode bash -c "hdfs dfsadmin -safemode leave"

## Create hdfs input directory - checked
echo ""
echo "[SCRIPT] Creating hdfs input directory"
echo ""
docker compose exec namenode bash -c "hdfs dfs -mkdir -p /data/anime_raw/mal"
docker compose exec namenode bash -c "hdfs dfs -mkdir -p /data/anime_raw/2023"
# docker compose exec namenode bash -c "hdfs dfs -mkdir -p /data/anime_treated/mal"
# docker compose exec namenode bash -c "hdfs dfs -mkdir -p /data/anime_treated/2023"
docker compose exec namenode bash -c "hdfs dfs -mkdir -p /data/anime_treated"

## Copy data files to hdfs input directory - Checked
echo ""
echo "[SCRIPT] Copying data files to hdfs input directory"
echo ""
docker compose exec namenode bash -c "hdfs dfs -put /input/mal/anime.csv /data/anime_raw/mal/anime.csv"
docker compose exec namenode bash -c "hdfs dfs -put /input/2023/anime.csv /data/anime_raw/2023/anime.csv"

# Spark : 
## start spark with /spark.py script - checked
echo ""
echo "[SCRIPT] Starting spark with /spark.py script"
echo ""
docker compose exec spark-master bash -c "/spark/bin/spark-submit --master 'spark://spark-master:7077' /spark.py" 

# Hive :
## Start HiveServer2 - checked
echo ""
echo "[SCRIPT] Starting HiveServer2"
echo ""
docker compose exec hive-server bash -c "hiveserver2"

## Check if HiveServer2 is up - checked
echo ""
echo "[SCRIPT] Checking if HiveServer2 is up"
echo ""
docker exec hive-server bash -c "while true; do if ! nc -z localhost 10000; then echo \"Waiting for HiveServer2 to start...\"; sleep 1; else break; fi; done"

## use beeline to connect to HiveServer2, then run /init.sql file
echo ""
echo "[SCRIPT] Connecting to HiveServer2 and running /init.sql file"
echo ""
docker compose exec hive-server bash -c "beeline -u jdbc:hive2://localhost:10000 -f /init.sql"

echo ""
echo "[SCRIPT] Done"
echo ""

docker compose exec namenode bash -c "hdfs dfs -ls /data/anime_treated/mal"