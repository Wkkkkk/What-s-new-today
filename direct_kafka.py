from __future__ import print_function
import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from get_tweets import create_url, connect_to_endpoint


def extract_id(content):
    json_content = json.loads(content)
    id = json_content["data"]["id"]

    return id


def extract_tag(content):
    json_content = json.loads(content)
    rules = json_content["matching_rules"]
    tags = [rule["tag"] for rule in rules]

    return tags


def get_post(id):
    url = create_url([id])
    json_response = connect_to_endpoint(url)

    return json.dumps(json_response, indent=4, sort_keys=True)


def main():
    sc = SparkContext(appName="PythonStreamingDirectKafka")
    ssc = StreamingContext(sc, 2)

    brokers, topic = "localhost:9092", "tweets"
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # count trend tags
    contents = kvs.map(lambda x: x[1])
    counts = contents.flatMap(extract_tag) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    # get all posts
    posts = contents.map(extract_id) \
        .window(60, 10) \
        .map(get_post)
    posts.pprint()

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
