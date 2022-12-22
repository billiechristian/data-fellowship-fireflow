# Streaming Transaction Data to Bigquery using Kafka

## How to run the program?

1. Clone this repository

2. Open table_id.json in json folder, change the the project_id with your project_id in GCP, change dataset_id and table_id with names that don't already exist in the dataset and table in your Bigquery. 

3. Still in the json folder, change key.json with your own key, don't forget to change the file name to key.json

4. Open Command Prompt in this directory, and type this command below to dockerize the Confluent Kafka:

```sh
docker compose -f docker-compose.yml up
```

5. Create a python virtual environment, activate it, and install the dependencies inside requirements.txt, you can use this command:

```sh
pip install -r requirements.txt
```

6. To run the producer, type this command in the CMD:

```sh
python3 producer.py
```

7. To run the consumer, type this command in the CMD:

```sh
python3 consumer.py
```

8. If you failed to install the dependencies in step 5, you can use Windows Subsystem for Linux (WSL) as terminal in steps 5 through 7 instead of using Command Prompt.

## Result

![](img/producer.png)<br>

![](img/consumer.png)<br>

![](img/bigquery.png)<br>
