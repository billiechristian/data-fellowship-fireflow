from confluent_kafka.avro import AvroConsumer
from google.cloud import bigquery
import os
import json

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= 'json/key.json'

with open("json/table_id.json", "r") as f:
    input_data = json.load(f)
    project_id = input_data["project_id"]
    dataset_id = input_data["dataset_id"]
    table_id = input_data["table_id"]

client = bigquery.Client()

def get_or_create_dataset():
    try : 
        dataset = client.get_dataset(project_id+"."+dataset_id)
    except:
        try :
            dataset = client.create_dataset(client.dataset(dataset_id = dataset_id, project=project_id))
            print("Success to create a Bigquery dataset.")
        except : 
            print("Failed to create a Bigquery table.")

def get_or_create_table_big_query():
    try :
        table = client.get_table(project_id+"."+dataset_id+"."+table_id)
    except :
        schema = [
                  bigquery.SchemaField("Date", "STRING", mode="REQUIRED"),
                  bigquery.SchemaField("User_ID", "INTEGER", mode="REQUIRED"),
                  bigquery.SchemaField("Transaction", "STRING", mode="REQUIRED"),
                  bigquery.SchemaField("Nominal", "INTEGER", mode="REQUIRED")
                    ]
        try :
            table = bigquery.Table(project_id+"."+dataset_id+"."+table_id, schema=schema)
            table = client.create_table(table)  # Make an API request.
            print("Success to create a Bigquery table.")
        except :
            print("Failed to create a Bigquery table.")

def ingest_bigquery(message):
    try:
        client.insert_rows_json(project_id+"."+dataset_id+"."+table_id, [message.value()])
        print("insert data success")
    except:
        print("insert data failed, try to insert data again")
        ingest_bigquery(message)

def read_messages():
    consumer_config = {"bootstrap.servers": "localhost:9092",
                       "schema.registry.url": "http://localhost:8081",
                       "group.id": "customers.transactions.avro.consumer.2",
                       "auto.offset.reset": "earliest"}

    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["customers.transactions"])
    get_or_create_dataset()
    get_or_create_table_big_query()

    while(True):
        try:
            message = consumer.poll(5)
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if message:
                print(f"Successfully poll a record from "
                      f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                      f"message key: {message.key()} || message value: {message.value()}")
                consumer.commit()
                ingest_bigquery(message)
            else:
                print("No new messages at this point. Try again later.")

    consumer.close()


if __name__ == "__main__":
    read_messages()

