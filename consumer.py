import json
from typing import Optional
from kafka import KafkaConsumer
from kafka.consumer.group import TopicPartition
from models import User
from config import TOPIC, PARTITION, KAFKA_SERVER


def main():
    print("Starting consumer.")

    message_offset: Optional[int] = 20

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_SERVER,
    )

    if message_offset is not None:
        topic_partition = TopicPartition(
            topic=TOPIC,
            partition=PARTITION,
        )
        consumer.assign([topic_partition])
        consumer.seek(
            partition=topic_partition,
            offset=message_offset,
        )
    else:
        consumer.subscribe(TOPIC)

    for msg in consumer:
        print("Received message:", msg)
        user = User(**json.loads(msg.value))
        print("Parsed object:", user)


if __name__ == "__main__":
    main()
