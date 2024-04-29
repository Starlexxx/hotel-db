import os
import json
import logging
import time

from elasticsearch import Elasticsearch
from py2neo import Graph, Node, Relationship

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_env_variable(var_name, default_value):
    return os.getenv(var_name, default_value)


def create_es_connection():
    es_url = get_env_variable('ES_URL', 'http://localhost:9200')
    return Elasticsearch(es_url)


def create_graph_connection():
    graph_db_url = get_env_variable('GRAPH_DB_URL', 'bolt://localhost:7687')
    graph_db_user = get_env_variable('GRAPH_DB_USER', 'neo4j')
    graph_db_password = get_env_variable('GRAPH_DB_PASSWORD', 'newpassword')
    return Graph(graph_db_url, auth=(graph_db_user, graph_db_password))


def load_json_data(file_name):
    with open(file_name, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


def index_data(es, index_name, data):
    for item in data:
        es.index(index=index_name, id=item["index"], body=item["body"])
    logger.info(f"Data indexed")


def clear_neo4j(graph):
    graph.run("MATCH (n) DETACH DELETE n")
    logger.info("Neo4j database cleared")


def enrich_neo4j(es, graph):
    response = es.search(index="hotel", body={"query": {"match_all": {}}})

    clients = []
    rooms = []

    print(response['hits']['hits'])
    for hit in response['hits']['hits']:
        source = hit["_source"]
        if "id_клиента" in source:
            clients.append(source)
        elif "описание_номера" in source:
            rooms.append(source)

    print(clients)
    print(rooms)

    for client in clients:
        client_node = Node("Client", id=client["id_клиента"], arrival_date=client["дата_прибытия"],
                           **client["карточка_регистрации"])  # unpack the dictionary into separate properties
        graph.create(client_node)
        for room in rooms:
            if room["id"] == client["id_номера"]:
                room_node = Node("Room", id=room["id"], price=room["стоимость_день"])
                graph.create(room_node)
                stay_relationship = Relationship(client_node, "STAYED", room_node,
                                                 duration=client["продолжительность_проживания"])
                graph.create(stay_relationship)
                print(
                    f"Client {client['id_клиента']} stayed in room {room['id']} for {client['продолжительность_проживания']} days")
                break

    logger.info("Data enriched")


def main():
    es = create_es_connection()
    if not es.ping():
        logger.error("Elasticsearch server is not running")
        exit(1)

    es.indices.delete(index="hotel", ignore=[400, 404])

    index_name = "hotel"
    settings = {
        "settings": {
            "analysis": {
                "filter": {
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "russian_stemmer": {
                        "type": "stemmer",
                        "language": "russian"
                    }
                },
                "analyzer": {
                    "russian": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "russian_stop",
                            "russian_stemmer"
                        ]
                    }
                }
            }
        }
    }
    es.indices.create(index=index_name, body=settings)

    client_mapping = {
        "properties": {
            "id_клиента": {"type": "integer"},
            "карточка_регистрации": {
                "properties": {
                    "Фамилия": {"type": "text", "analyzer": "russian"},
                    "Имя/Отчество": {"type": "text", "analyzer": "russian"},
                    "Дата рождения": {"type": "date"},
                    "Пол": {"type": "text", "analyzer": "russian"},
                    "Место рождения": {"type": "text", "analyzer": "russian"},
                    "Документ, удостоверяющий личность": {"type": "text", "analyzer": "russian"},
                    "Серия/номер": {"type": "integer"},
                    "Дата выдачи": {"type": "date"},
                    "Код подразделения": {"type": "text"},
                    "Кем выдан": {"type": "text", "analyzer": "russian"},
                    "Адрес места жительства (регистрации)": {"type": "text", "analyzer": "russian"},
                }
            },
            "дата_прибытия": {"type": "date"},
            "продолжительность_проживания": {"type": "integer"},
            "услуга": {"type": "text", "analyzer": "russian"},
            "id_номера": {"type": "integer"}
        }
    }
    room_mapping = {
        "properties": {
            "описание_номера": {"type": "text", "analyzer": "russian"},
            "стоимость_день": {"type": "integer"}
        }
    }
    es.indices.put_mapping(index=index_name, body=client_mapping)
    es.indices.put_mapping(index=index_name, body=room_mapping)

    clients = load_json_data('clients.json')
    rooms = load_json_data('rooms.json')

    index_data(es, "hotel", clients)
    index_data(es, "hotel", rooms)

    graph = create_graph_connection()
    time.sleep(20)
    clear_neo4j(graph)
    enrich_neo4j(es, graph)


if __name__ == "__main__":
    main()
