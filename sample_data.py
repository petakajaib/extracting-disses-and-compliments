import json
import pickle
from pymongo import MongoClient
from settings import *


def transform(article_):

    article = {}

    for key, value in article_.items():

        if key == "_id":
            continue

        article[key] = value

    return article

def export_sample(collection, sample_data_path="data/sample.pkl"):
    sample_data = {"data": []}

    for article_ in collection.aggregate([{"$match":{}}, {"$sample": {"size": 100}}]):

        article = transform(article_)
        print(article)

        sample_data["data"].append(article)

    pickle.dump(sample_data, open(sample_data_path, "wb"))

def import_sample(collection, sample_data_path="data/sample.pkl"):

    sample_data = pickle.load(open(sample_data_path, "rb"))

    for article in sample_data["data"]:
        print(article)
        if collection.count(article) == 0:
            print(article)
            collection.insert_one(article)



if __name__ == '__main__':

    client = MongoClient()

    db = client[MONGO_DB]
    collection = db[ARTICLE_COLLECTION]

    #export_sample(collection)

    import_sample(collection)
