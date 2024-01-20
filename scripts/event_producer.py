import json
import uuid
import os
import json
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from faker import Faker
from time import sleep
from datetime import datetime, timedelta

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:9092")
faker = Faker()

class DataGenerator(object):
    @staticmethod
    def get_data():
        now = datetime.now()
        jabodetabek_cities = ["Jakarta", "Bogor", "Depok", "Tangerang", "Bekasi"]
        social_media_preferences = ["Instagram", "TikTok", "X", "Facebook", "Telegram"]

        return [
            uuid.uuid4().__str__(),
            faker.random_element(elements=jabodetabek_cities),
            faker.random_element(elements=social_media_preferences),
            faker.random_int(min=50, max=1000),
            faker.random_int(min=50, max=150),
            faker.unix_time(
                start_datetime=now - timedelta(days=7), end_datetime=now
            ),
        ]

while True:
    columns = [
        "ID",
        "Kota",
        "Sosial_Media",
        "Usage_Count",
        "Sales",
        "Ts",
    ]
    data_list = DataGenerator.get_data()

    # Set usage_count based on social media preferences
    social_media = data_list[2]
    if social_media == "Instagram":
        data_list[3] = faker.random_int(min=500, max=1000)
    elif social_media == "TikTok":
        data_list[3] = faker.random_int(min=300, max=700)
    elif social_media == "X":
        data_list[3] = faker.random_int(min=200, max=500)
    elif social_media == "Facebook":
        data_list[3] = faker.random_int(min=100, max=300)
    else:  # Telegram
        data_list[3] = faker.random_int(min=50, max=150)

    json_data = dict(zip(columns, data_list))
    _payload = json.dumps(json_data).encode("utf-8")
    print(_payload, flush=True)
    print("=-" * 5, flush=True)
    response = producer.send(topic=kafka_topic, value=_payload)
    print(response.get())
    print("=-" * 20, flush=True)
    sleep(5)
