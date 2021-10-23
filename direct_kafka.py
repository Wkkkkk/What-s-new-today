from __future__ import print_function
import sys
import json
from time import time

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from influxdb import InfluxDBClient

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

    all_response = connect_to_endpoint(url) # dic
    all_response = all_response.get("data", [])

    posts = []
    for response in all_response:
        post = {"measurement": "twitters"}
        post["time"] = response["created_at"]

        fields = {}
        fields["author_id"] = response["author_id"]
        fields["id"] = response["id"]
        fields["lang"] = response["lang"]
        fields["source"] = response["source"]
        fields["text"] = response["text"]
        fields["created_at"] = response["created_at"]
        post["fields"] = fields

        tags = {}
        tags["context_annotations"] = response.get("context_annotations", [])
        tags["public_metrics"] = response.get("public_metrics", {})
        post["tags"] = tags
        
        print(">>POST:", post)
        posts.append(post)

    return posts


def main():
    client = InfluxDBClient(host='localhost', port=8086)
    client.create_database('pyexample')
    client.switch_database('pyexample')

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
    posts = contents.window(10, 6) \
        .map(extract_id) \
        .map(get_post) \
        .map(lambda post: client.write_points(post))
    posts.pprint()

    ssc.start()
    ssc.awaitTermination()
    client.close()


if __name__ == "__main__":
    main()
