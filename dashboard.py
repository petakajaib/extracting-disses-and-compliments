import json
import requests
from requests.auth import HTTPBasicAuth
from settings import *

api_credentials = json.load(open("api_credentials.json"))

http_user = api_credentials["user"]
http_password = api_credentials["password"]

def rename_key(data, original_name, new_name):

    published_date = data[original_name]
    del(data[original_name])
    data[new_name] = published_date

    return data

def transform(data_):

    data = data_.copy()

    data = rename_key(data, "created_at", "published_date")
    data = rename_key(data, "entity", "name")
    data = rename_key(data, "entity_tag", "tag")
    data = rename_key(data, "positive_sentiment", "positive")
    data = rename_key(data, "negative_sentiment", "negative")

    return data

def rest_request(url, request):

    res = requests.post(url, json=request, auth=HTTPBasicAuth(http_user, http_password))
    parsed = json.loads(res.content)

    return parsed


def get_entity(data):

    entity_url = "{}/entities.json".format(DASHBOARD_BASE_URL)
    entity_request = {"name": data["name"], "tag": data["tag"]}

    return rest_request(entity_url, entity_request)

def get_source(data):

    source_url = "{}/sources.json".format(DASHBOARD_BASE_URL)
    source_request = {
        "published_date": data["published_date"],
        "url": data["url"],
        "month": data["month"],
        "day": data["day"],
        "year": data["year"]
        }

    return rest_request(source_url, source_request)

def get_entity_source(data, entity, source):

    entity_sources_url = "{}/entity_sources.json".format(DASHBOARD_BASE_URL)
    entity_source_request = {
        "entity_id": entity["id"],
        "source_id": source["id"]
    }

    return rest_request(entity_sources_url, entity_source_request)

def get_sentence(data, source):

    sentence_url = "{}/sentences.json".format(DASHBOARD_BASE_URL)
    sentence_request = {
        "source_id": source["id"],
        "sentence": data["sentence"],
        "language_code": data["language_code"],
        "tokens_count": data["tokens_count"]
    }

    return rest_request(sentence_url, sentence_request)


def get_entity_sentence(data, entity, sentence):

    entity_sentence_url = "{}/entity_sentences.json".format(DASHBOARD_BASE_URL)
    entity_sentence_request = {
        "entity_id": entity["id"],
        "sentence_id": sentence["id"]
    }

    return rest_request(entity_sentence_url, entity_sentence_request)

def get_sentiment(data, entity, sentence, source):

    sentiment_url = "{}/sentiments.json".format(DASHBOARD_BASE_URL)

    sentiment_request = {
        "positive": data["positive"],
        "negative": data["negative"],
        "entity_id": entity["id"],
        "sentence_id": sentence["id"],
        "source_id": source["id"],
        "sentiment": data["sentiment"]
    }

    return rest_request(sentiment_url, sentiment_request)


def execute_requests(data):

    entity = get_entity(data)
    source = get_source(data)
    sentence = get_sentence(data, source)
    entity_source = get_entity_source(data, entity, source)
    entity_sentence = get_entity_sentence(data, entity, sentence)
    sentiment = get_sentiment(data, entity, sentence, source)


def insert_to_dashboard(data):

    transformed = transform(data)

    execute_requests(transformed)
