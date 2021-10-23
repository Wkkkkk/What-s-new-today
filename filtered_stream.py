import requests
import os
import json
import tweepy
from kafka import KafkaProducer
from time import time

from tokens import access_token, access_token_secret, consumer_key, consumer_secret, bearer_token
# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
#    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
#    print(json.dumps(response.json()))


def get_trending(regioncode):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    trends = api.get_place_trends(regioncode)
    
    # dict which we'll put in data.
    data = trends[0] 
    # grab the trends
    trends = data['trends']
    # grab the name from each trend
    names = [trend['name'] for trend in trends]
    print("trends:", names)
    return names


def set_rules(trends):
    # You can adjust the rules if needed
#    sample_rules = [
#        {"value": "dog has:images", "tag": "dog pictures"},
#        {"value": "cat has:images -grumpy", "tag": "cat pictures"},
#    ]

    sample_rules = [{"value": x + " -is:retweet -is:quote -is:reply", "tag": x} for x in trends[:5]]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
#    print(json.dumps(response.json()))


def get_stream(producer):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
#    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            id = json_response["data"]["id"]
#            print(json.dumps(json_response, indent=4, sort_keys=True))
            json_response["timestamp"] = int(time())
            data = json.dumps(json_response, indent=4, sort_keys=True)
#            producer.send("tweets", key=id, value=data.encode("utf-8"))
            producer.send("tweets", data.encode("utf-8"))
            print(data)


def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    trends = get_trending(23424975) #UK
    set_rules(trends)

    producer = KafkaProducer()    
    get_stream(producer)


if __name__ == "__main__":
    main()
