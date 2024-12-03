import datetime
import json
import os
import datetime
from kafka import KafkaProducer

start_time = datetime.datetime.now()

filePath = os.path.join(os.path.dirname(__file__), "Video_Games.jsonl")

topic_name = "topicReviews"

start_date = datetime.datetime(year=2015, month=1, day=1)
start = start_date.timestamp() * 1000
end_date = datetime.datetime(year=2020, month=1, day=1)
end = end_date.timestamp() * 1000

print(f"Start: {start}, End: {end}")

# read file
reviews = []
with open(filePath, 'r') as fp:
    for line in fp:
        reviews.append(json.loads(line.strip()))

# filter file by date
reviews = [review for review in reviews if review['timestamp'] >= start and review['timestamp'] < end]

# define producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka broker(s)
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize value as UTF-8
)

# send on kafka topic
for review in reviews:
    print(review)
    producer.send(topic_name, review)
    producer.flush()

producer.close()

print(f"Done {len(reviews)} reviews in {datetime.datetime.now() - start_time}")
