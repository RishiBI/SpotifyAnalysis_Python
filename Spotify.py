# Databricks notebook source
# DBTITLE 1,import lib
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,create dataframe
df = spark.read.load('/FileStore/tables/spotify_2023-2.csv',format='csv',sep=',',header='true',escape='"',inferschema='true')

# COMMAND ----------

df.count()

# COMMAND ----------

df.show(1)

# COMMAND ----------

# DBTITLE 1,check schema
df.printSchema()

# COMMAND ----------

# DBTITLE 1,data cleaning
df=df.drop("speechiness_%","liveness_%","instrumentalness_%","acousticness_%","energy_%","valence_%","danceability_%","mode","key","bpm")

# COMMAND ----------

df.show(2)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col
`
df=df.withColumn("streams",col("streams").cast(IntegerType()))\
    .withColumn("in_deezer_playlists",col("in_deezer_playlists").cast(IntegerType()))\
        .withColumn("in_shazam_charts",col("in_shazam_charts").cast(IntegerType()))

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.createOrReplaceTempView("track_names")

# COMMAND ----------

# MAGIC %sql select * from track_names

# COMMAND ----------

# MAGIC %sql select track_name,sum(artist_count) from track_names
# MAGIC group by 1
# MAGIC order by 2 desc
# MAGIC

# COMMAND ----------

# MAGIC %sql select track_name,in_spotify_charts,sum(artist_count) from track_names
# MAGIC group by 1,2
# MAGIC order by 3 desc

# COMMAND ----------


