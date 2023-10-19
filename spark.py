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

    match month:
        case "Jan":
            return "Winter"
        case "Feb":
            return "Winter"
        case "Mar":
            return "Winter"
        case "Apr":
            return "Spring"
        case "May":
            return "Spring"
        case "Jun":
            return "Spring"
        case "Jul":
            return "Summer"
        case "Aug":
            return "Summer"
        case "Sep":
            return "Summer"
        case "Oct":
            return "Fall"
        case "Nov":
            return "Fall"
        case "Dec":
            return "Fall"

anime_2023 = anime_2023.withColumn("AiredSeason",  lit(getSeason(anime_2023["Aired"])))

# save to hdfs
anime_mal.write.csv("hdfs://namenode:9000/data/anime_treated/mal",
                    header=False)
anime_2023.write.csv("hdfs://namenode:9000/data/anime_treated/2023",
                     header=False)

spark.stop()