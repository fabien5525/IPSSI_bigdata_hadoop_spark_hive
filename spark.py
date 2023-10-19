from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

anime_mal = spark.read.csv("hdfs://namenode:9000/data/anime_raw/mal/anime.csv", header=True, inferSchema=True)
anime_2023 = spark.read.csv("hdfs://namenode:9000/data/anime_raw/2023/anime.csv", header=True, inferSchema=True)

# Delete rows with null anime_id
anime_mal = anime_mal.na.drop(subset=["anime_id"])
anime_2023 = anime_2023.na.drop(subset=["anime_id"])

# save to hdfs
anime_mal.write.csv("hdfs://namenode:9000/data/anime_treated/mal", header=False)
anime_2023.write.csv("hdfs://namenode:9000/data/anime_treated/2023", header=False)

spark.stop()