from pyspark.sql import SparkSession
import re
from pyspark.sql.types import *
from pyspark.sql.functions import lit, udf

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

anime_mal = spark.read.csv("hdfs://namenode:9000/data/anime_raw/mal/anime.csv",
                           header=True,
                           inferSchema=True)
anime_2023 = spark.read.csv(
    "hdfs://namenode:9000/data/anime_raw/2023/anime.csv",
    header=True,
    inferSchema=True)

# Delete rows with null anime_id
anime_mal = anime_mal.na.drop(subset=["anime_id"])
anime_2023 = anime_2023.na.drop(subset=["anime_id"])

pattern = r'\d{4}'


@udf(returnType=StringType())
def getYear(colonne):
    match = re.search(pattern, colonne)
    if match:
        # Extract and print the matched year
        first_year = match.group()
        return first_year


anime_2023 = anime_2023.withColumn("AiredYear",
                                   lit(getYear(anime_2023["Aired"])))

@udf(returnType=StringType())
def getSeason(colonne):
    
    month = colonne[0:3]

    if month == "Jan":
        return "Winter"
    if month == "Feb":
        return "Winter"
    if month == "Mar":
        return "Winter"
    if month == "Apr":
        return "Spring"
    if month == "May":
        return "Spring"
    if month == "Jun":
        return "Spring"
    if month == "Jul":
        return "Summer"
    if month == "Aug":
        return "Summer"
    if month == "Sep":
        return "Summer"
    if month == "Oct":
        return "Fall"
    if month == "Nov":
        return "Fall"
    if month == "Dec":
        return "Fall"
    return "Other"

anime_2023 = anime_2023.withColumn("AiredSeason",  lit(getSeason(anime_2023["Aired"])))

# save to hdfs
anime_mal.write.csv("hdfs://namenode:9000/data/anime_treated/mal",
                    header=False)
anime_2023.write.csv("hdfs://namenode:9000/data/anime_treated/2023",
                     header=False)

spark.stop()