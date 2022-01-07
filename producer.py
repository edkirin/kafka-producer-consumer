import uuid
from kafka import KafkaProducer
from models import User
from config import TOPIC, PARTITION, KAFKA_SERVER


def main():
    print("Sending message...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
    )

    user = User(
        uuid=str(uuid.uuid4()),
        first_name="Pero",
        last_name="Perić",
        email="pero@example.com",
    )
    print("Object:", user)

    producer.send(
        topic=TOPIC,
        value=user.json().encode(),
        partition=PARTITION,
    )


if __name__ == "__main__":
    main()
