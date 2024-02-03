import json
import uuid
import os
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from faker import Faker
from time import sleep
from datetime import datetime, timedelta

# Load environment variables from .env file
dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

# Retrieve Kafka server host and topic from environment variables
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

# Create Kafka producer and Faker instances
producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:9092")
faker = Faker()

# DataGenerator class for generating synthetic data
class DataGenerator(object):
    @staticmethod
    def get_data():
        # Get current timestamp
        now = datetime.now()

        # Define possible values for Jabodetabek cities and social media preferences
        jabodetabek_cities = ["Jakarta", "Bogor", "Depok", "Tangerang", "Bekasi"]
        social_media_preferences = ["Instagram", "TikTok", "X", "Facebook", "Telegram"]

        # Generate random data for each field
        return [
            uuid.uuid4().__str__(),
            faker.random_element(elements=jabodetabek_cities),
            faker.random_element(elements=social_media_preferences),
            faker.random_int(min=50, max=1000),     # Usage_Count
            faker.random_int(min=50, max=150),      # Sales
            faker.random_int(min=500, max=1000),    # Visitors
            faker.random_int(min=50, max=250),      # PageVisitors
            faker.random_int(min=100, max=500),     # Conversion
            faker.unix_time(
                start_datetime=now - timedelta(minutes=5), end_datetime=now
            ),  # Timestamp (Ts)
        ]

# Infinite loop for continuously generating and sending data to Kafka
while True:
    # Define column names for the generated data
    columns = [
        "ID",
        "Kota",
        "Sosial_Media",
        "Usage_Count",
        "Sales",
        "Visitors",
        "PageVisitors",
        "Conversion",
        "Ts",
    ]

    # Generate synthetic data
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

    # Create a dictionary from column names and generated data
    json_data = dict(zip(columns, data_list))

    # Convert the dictionary to JSON and encode as bytes
    _payload = json.dumps(json_data).encode("utf-8")

    # Print payload for debugging
    print(_payload, flush=True)
    print("=-" * 5, flush=True)

    # Send the payload to the Kafka topic
    response = producer.send(topic=kafka_topic, value=_payload)

    # Print Kafka response for debugging
    print(response.get())
    print("=-" * 20, flush=True)

    # Sleep for 5 seconds before generating the next data (configurable)
    sleep(5)
