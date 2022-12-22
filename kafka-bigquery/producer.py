
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from time import sleep
import radar
from faker import Faker
import random

fake=Faker()

def load_avro_schema_from_file():
    key_schema = avro.load("avro/customers_transactions_key.avsc")
    value_schema = avro.load("avro/customers_transactions_value.avsc")

    return key_schema, value_schema


def send_record():
    key_schema, value_schema = load_avro_schema_from_file()

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1"
    }

    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)

    
    while True:
        date = str(radar.random_datetime(start='2021-01-01', stop='2021-12-31'))
        user_id = random.choice([i for i in range(1,101)])
        key = {"Date": date}
        transaction = random.choice(["Debit", "Credit"])
        nominal = fake.random_int(min=1, max=1000, step=1)
        value = {"Date": date, "User_ID": user_id, "Transaction": transaction, "Nominal": nominal }

        try:
            producer.produce(topic='customers.transactions', key=key, value=value)
        except Exception as e:
            print(f"Exception while producing record value - {value}: {e}")
        else:
            print(f"Successfully producing record value - {value}")

        producer.flush()
        sleep(random.choice([i for i in range(1,11)]))

if __name__ == "__main__":
    send_record()
