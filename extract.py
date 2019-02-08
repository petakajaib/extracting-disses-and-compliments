import re
from polyglot.text import Text
import pycld2
from pymongo import MongoClient
from dashboard import insert_to_dashboard
from settings import *

sentence_meta_keys = [
    "created_at",
    "month",
    "day",
    "year",
    "url",
    "twitter_handle",
    "publish_date"
]

supported_languages = ["ms", "en"]

def get_clean_content(content):

    content = content.replace('“', "\"")
    content = content.replace('”', "\"")
    content = content.replace("‘", "'")
    content = content.replace("’", "'")
    content = content.replace("…", ".")
    content = re.sub("\s+", " ", content)

    return content

def get_sentence_meta(article):

    sentence_meta_info = {}

    for key in sentence_meta_keys:

        sentence_meta_info[key] = article[key]

    return sentence_meta_info

def entity_polarity_generator_(article):
    entry = get_sentence_meta(article)
    content = get_clean_content(article["content"])

    try:
        detection = pycld2.detect(content)


        detection_is_reliable, _, languages = detection

        if detection_is_reliable:

            detected_language = languages[0][1]
            entry["language_code"] = detected_language

            if detected_language in supported_languages:


                parsed = Text(content)

                for sentence in parsed.sentences:

                    # if len(sentence.tokens) > 15:
                    #     continue
                    entry["tokens_count"] = len(sentence.tokens)
                    entry["sentence"] = str(sentence)

                    for entity in sentence.entities:

                        entry["entity"] = " ".join(entity)
                        entry["entity_tag"] = entity.tag
                        try:
                            entry["positive_sentiment"] = entity.positive_sentiment
                            entry["negative_sentiment"] = entity.negative_sentiment

                            if entry["positive_sentiment"] > 0.0:
                                entry["sentiment"] = "positive"
                            elif entry["negative_sentiment"] > 0.0:
                                entry["sentiment"] = "negative"


                        except IndexError as err:
                            continue

                        yield entry
    except pycld2.error:
        yield None

def entity_polarity_generator(article):

    for entry in entity_polarity_generator_(article):
        if entry:
            if entry.get("_id"):
                del(entry["_id"])
            yield entry

if __name__ == '__main__':


    query = {
        "created_at": {"$exists": True},
        "month": {"$exists": True},
        "day": {"$exists": True},
        "year": {"$exists": True},
        "url": {"$exists": True},
        "content": {"$exists": True},
        "sentiment_extracted": False
    }

    client = MongoClient()
    db = client[MONGO_DB]

    article_collection = db[ARTICLE_COLLECTION]

    sentiment_collection = db[SENTIMENT_COLLECTION]

    article_collection.update_many({"sentiment_extracted": {"$exists": False}}, {"$set": {"sentiment_extracted": False}})


    pipeline = [{"$match": query}, {"$sample": {"size": 1000}}]

    sentiment_extracted_false_count = article_collection.count(query)
    idx = 0
    while sentiment_extracted_false_count > 0:

        print("sentiment_extracted_false_count:", sentiment_extracted_false_count)

        article_ids = [article["_id"] for article in article_collection.aggregate(pipeline)]

        article_collection.update_many({"_id": {"$in": article_ids}}, {"$set": {"sentiment_extracted": True}})

        for article_id in article_ids:
            article = article_collection.find_one({"_id": article_id})
            for entry in entity_polarity_generator(article):
                print(entry["entity"], idx)

                insert_to_dashboard(entry)
                sentiment_collection.insert_one(entry)
                idx += 1

        sentiment_extracted_false_count = article_collection.count(query)
