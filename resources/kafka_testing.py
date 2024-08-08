#skapa kafka server
# docker-compose up

# köra kommandon i terminal
#docker exec -it [container_name_or_id] /bin/sh]
# docker exec -it dockerapachekafka-kafka-1 /bin/sh

# Skapa en kafka topic
# kafka-topics.sh --create --topic [topic_name] --bootstrap-server localhost:9092 --partitions [num_partitions] --replication-factor [num_replicas]
#Exempel: kafka-topics.sh --create --topic my_app_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Listar alla kafka topics
# kafka-topics.sh --list --bootstrap-server localhost:9092 

# kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my_app_topic --from-beginning
from kafka import KafkaProducer, KafkaConsumer
import json
producer = KafkaProducer(bootstrap_servers='localhost:9092')
while True:
    msg = input("skriv något ")
    if msg == 'q': break
    producer.send('test_topic', bytes(msg,'utf-8'))

# kafka-topics.sh --create --topic processed_orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1