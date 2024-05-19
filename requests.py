import logging
from elasticsearch import Elasticsearch
from py2neo import Graph

from db_processor import get_env_variable, create_graph_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_es_instance():
    es = Elasticsearch('http://localhost:9200')
    if not es.ping():
        raise Exception("Elasticsearch server is not running")
    return es


def check_index_exists(es, index):
    if not es.indices.exists(index=index):
        raise Exception(f"Index {index} does not exist")


def main():
    es = create_es_instance()
    check_index_exists(es, "client")
    check_index_exists(es, "room")

    logger.info("Clients in rooms: %s", clients_in_rooms(es))
    logger.info("Number of lux rooms: %s", number_of_lux_rooms(es))
    logger.info("Client with max cost: %s", get_client_with_max_cost())


def clients_in_rooms(es):
    query = {
        "size": 0,
        "aggs": {
            "clients_by_arrival_date": {
                "date_histogram": {
                    "field": "дата_прибытия",
                    "calendar_interval": "year"
                },
                "aggs": {
                    "clients_by_room": {
                        "terms": {
                            "field": "id_номера"
                        }
                    }
                }
            }
        }
    }
    return es.search(index="client", body=query)


def number_of_lux_rooms(es):
    query = {
        "query": {
            "match_phrase": {
                "описание_номера": {
                    "query": "Мы рады предложить",
                    "slop": 0
                }
            }
        }
    }
    return es.search(index="room", body=query)


def get_client_with_max_cost():
    graph = create_graph_connection()
    query = """
    MATCH (c:Client)-[r:STAYED]->(room:Room)
    WITH c, sum(r.duration * room.стоимость_день) AS total_cost
    ORDER BY total_cost DESC
    RETURN c LIMIT 1
    """
    result = graph.run(query)
    return result.data()


if __name__ == "__main__":
    main()
