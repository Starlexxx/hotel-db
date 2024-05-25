# /spark/bin/spark-submit spark_request.py in spark-master container

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Hotel").getOrCreate()
clients = spark.read.csv("hdfs://namenode:9000/data/hotel/clients.csv", header=True, inferSchema=True)
rooms = spark.read.csv("hdfs://namenode:9000/data/hotel/rooms.csv", header=True, inferSchema=True)
stayed = spark.read.csv("hdfs://namenode:9000/data/hotel/stayed.csv", header=True, inferSchema=True)

clients.createOrReplaceTempView("clients")
rooms.createOrReplaceTempView("rooms")
stayed.createOrReplaceTempView("stayed")

rooms_stayed = spark.sql("SELECT room_id, COUNT(*) as total_stayed FROM stayed GROUP BY room_id")
rooms_stayed.show()
input()
