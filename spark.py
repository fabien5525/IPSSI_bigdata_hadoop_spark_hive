from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

anime_mal = spark.read.csv("hdfs://namenode:9000/data/anime/mal/anime.csv", header=True, inferSchema=True)
anime_2023 = spark.read.csv("hdfs://namenode:9000/data/anime/2023/anime.csv", header=True, inferSchema=True)

# anime_mal.show()
# anime_2023.show()

spark.stop()