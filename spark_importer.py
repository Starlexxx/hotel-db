from datetime import datetime, timedelta

from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Hotel").getOrCreate()
es = Elasticsearch('http://localhost:9200')
CLIENT_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("карточка_регистрации", StringType(), False),
    StructField("дата_прибытия", DateType(), False),
    StructField("продолжительность_проживания", IntegerType(), False),
    StructField("услуга", StringType(), False),
    StructField("id_номера", IntegerType(), False)
])

ROOM_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("описание_номера", StringType(), False),
    StructField("стоимость_день", IntegerType(), False)
])

STAYED_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("client_id", IntegerType(), False),
    StructField("room_id", IntegerType(), False),
    StructField("check_in", DateType(), False),
    StructField("check_out", DateType(), False)
])


def fetch_data_from_es(index, size=30):
    try:
        return es.search(index=index, body={"query": {"match_all": {}}}, size=size)
    except Exception as e:
        print(f"Error fetching data from {index}: {e}")
        return None


def generate_stay_data(clients, rooms):
    stayed = []
    for client in clients['hits']['hits']:
        room = next((r for r in rooms['hits']['hits'] if r["_source"]["id"] == client["_source"]["id_номера"]), None)
        if room:
            check_in_date = datetime.strptime(client["_source"]["дата_прибытия"], "%d/%m/%Y")
            check_out_date = check_in_date + timedelta(days=client["_source"]["продолжительность_проживания"])
            stayed.append({
                "client": client["_source"]["id"],
                "room": room["_source"]["id"],
                "check_in": check_in_date.strftime("%Y-%m-%d"),
                "check_out": check_out_date.strftime("%Y-%m-%d")
            })
    return stayed


def process_client_data(client):
    return (
        client["_source"].get("id_клиента", None),
        client["_source"].get("карточка_регистрации", None),
        datetime.strptime(client["_source"].get("дата_прибытия", "1900-01-01"), "%d/%m/%Y"),
        client["_source"].get("продолжительность_проживания", None),
        client["_source"].get("услуга", None),
        client["_source"].get("id_номера", None)
    )


def process_room_data(room):
    return (
        room["_source"].get("id", None),
        room["_source"].get("описание_номера", None),
        room["_source"].get("стоимость_день", None)
    )


def process_stay_data(stay, i):
    return (
        i,
        stay["client"],
        stay["room"],
        datetime.strptime(stay["check_in"], "%Y-%m-%d"),
        datetime.strptime(stay["check_out"], "%Y-%m-%d")
    )


def main():
    clients = fetch_data_from_es("client")
    rooms = fetch_data_from_es("room")
    stayed = generate_stay_data(clients, rooms)

    if not clients or not rooms:
        print("Error fetching data, exiting.")
        return

    clients_data = [process_client_data(client) for client in clients['hits']['hits']]
    rooms_data = [process_room_data(room) for room in rooms['hits']['hits']]
    stayed_data = [process_stay_data(stay, i) for i, stay in enumerate(stayed)]

    clients_df = spark.createDataFrame(data=clients_data, schema=CLIENT_SCHEMA)
    rooms_df = spark.createDataFrame(data=rooms_data, schema=ROOM_SCHEMA)
    stayed_df = spark.createDataFrame(data=stayed_data, schema=STAYED_SCHEMA)

    clients_df.repartition(1).write.csv(path='exports/clients.csv', mode='overwrite', header=True)
    rooms_df.repartition(1).write.csv(path='exports/rooms.csv', mode='overwrite', header=True)
    stayed_df.repartition(1).write.csv(path='exports/stayed.csv', mode='overwrite', header=True)

    clients_df.write.csv(path='hdfs://namenode:9000/clients.csv', mode='overwrite', header=True)
    rooms_df.write.csv(path='hdfs://namenode:9000/rooms.csv', mode='overwrite', header=True)
    stayed_df.write.csv(path='hdfs://namenode:9000/stayed.csv', mode='overwrite', header=True)


if __name__ == "__main__":
    main()
