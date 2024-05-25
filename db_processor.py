import os
import json
import logging

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
        es.index(index=index_name, id=item["index"], body={**item["body"], "id": item["id"]})
    es.indices.refresh(index=index_name)
    logger.info(f"Data indexed in {index_name}")


def clear_neo4j(graph):
    graph.run("MATCH (n) DETACH DELETE n")
    logger.info("Neo4j database cleared")


def enrich_neo4j(es, graph):
    response_clients = es.search(index="client", body={"query": {"match_all": {}}}, size=30)
    response_rooms = es.search(index="room", body={"query": {"match_all": {}}}, size=30)

    clients = [hit["_source"] for hit in response_clients['hits']['hits']]
    rooms = [hit["_source"] for hit in response_rooms['hits']['hits']]
    client_room = {}
    for i, client in enumerate(clients):
        room = next((r for r in rooms if r.get("id") == client.get("id_номера")), None)
        if room:
            client_room[i] = room

    for client_i, room in client_room.items():
        client = clients[client_i]
        client_node = Node("Client", id=client["id"], arrival_date=client["дата_прибытия"],
                           **client["карточка_регистрации"])
        graph.create(client_node)
        room_node = Node("Room", id=room["id"], cost=room["стоимость_день"])
        graph.create(room_node)
        tx = graph.begin()
        try:
            relation_query = """
                MATCH (client:Client {id: $client_id}), (room:Room {id: $room_id})
                CREATE (client)-[:STAYED {duration: $duration}]->(room)
            """
            parameters = {
                "client_id": client["id"],
                "room_id": room["id"],
                "duration": client["продолжительность_проживания"]
            }
            tx.run(relation_query, parameters)
            logger.info(
                f"Client {client['id']} stayed in room {room['id']} for {client['продолжительность_проживания']} days")
        except Exception as e:
            logger.error(f"Failed to create relationship: {e}")
            logger.info(
                f"Client {client['id']} stayed in room {room['id']} for {client['продолжительность_проживания']} days")

    logger.info("Data enriched")


def main():
    es = create_es_connection()
    if not es.ping():
        logger.error("Elasticsearch server is not running")
        exit(1)

    es.indices.delete(index="room", ignore=[400, 404])
    es.indices.delete(index="client", ignore=[400, 404])

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
    es.indices.create(index="room", body=settings)
    es.indices.create(index="client", body=settings)

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
                    "Дата выдачи": {"type": "date", "format": "dd/MM/yyyy"},
                    "Код подразделения": {"type": "text"},
                    "Кем выдан": {"type": "text", "analyzer": "russian"},
                    "Адрес места жительства (регистрации)": {"type": "text", "analyzer": "russian"},
                }
            },
            "дата_прибытия": {"type": "date", "format": "dd/MM/yyyy"},
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

    es.indices.put_mapping(index="room", body=room_mapping)
    es.indices.put_mapping(index="client", body=client_mapping)

    rooms = load_json_data('rooms.json')
    clients = load_json_data('clients.json')

    index_data(es, "room", rooms)
    index_data(es, "client", clients)

    graph = create_graph_connection()
    clear_neo4j(graph)
    enrich_neo4j(es, graph)


if __name__ == "__main__":
    main()
