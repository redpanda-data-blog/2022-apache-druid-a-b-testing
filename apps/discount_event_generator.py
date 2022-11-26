import time
import json
import random
import os

from datetime import datetime
from kafka import KafkaProducer

topic_name = "sales-by-discount"

def read_ab_config():
    with open('./apps/ab_config', 'r') as f:
        lines = f.readlines()
        return int(lines[0])

def random_user_id():
    return f"user{random.randint(1, 100)}"

def random_id():
    discount_at_home_page_percentage= read_ab_config()
    id_list = ["discount-at-home-page", "discount-at-category-page"]

    return random.choices(id_list, weights=(discount_at_home_page_percentage, 100-discount_at_home_page_percentage), k=5)

def random_event_trigger_location():
    event_trigger_location_list = ["home", "fashion"]
    return random.choice(event_trigger_location_list)

def random_location():
    location1 = {}
    location1["place"] = "San Francisco"
    location1["region"] = {}
    location1["region"]["id"] = "us-west"
    location1["region"]["description"] = "US West"

    location2 = {}
    location2["place"] = "Boston"
    location2["region"] = {}
    location2["region"]["id"] = "us-east"
    location2["region"]["description"] = "US East"

    location3 = {}
    location3["place"] = "New York"
    location3["region"] = {}
    location3["region"]["id"] = "us-east"
    location3["region"]["description"] = "US East"

    location_list = [location1, location2, location3]
    return random.choice(location_list)


def get_json_data():
    data = {}

    data["event-time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    data["goal-id"] = topic_name
    data["id"] = random_id()
    data["event-trigger-location"] = random_event_trigger_location()
    data["user-id"] = random_user_id()
    data["miscellaneous-details"] = random_location()

    return json.dumps(data) 

def main():
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    for _ in range(20000):
        discount_at_home_page_percentage= read_ab_config()
        json_data = get_json_data()
        producer.send(topic_name, bytes(f'{json_data}','UTF-8'))
        print(f"Data is sent: {json_data}")
        print(f"discount-at-home-page (A): {discount_at_home_page_percentage}")
        print(f"discount-at-category-page (B): {100-discount_at_home_page_percentage}")
        time.sleep(5)


if __name__ == "__main__":
    main()
