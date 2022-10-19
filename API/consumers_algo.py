from kafka import KafkaConsumer, KafkaProducer
import os
import json
import uuid
from concurrent.futures import ThreadPoolExecutor

#EN ESTE ARCHIVO DEBERÍAN ESTAR LAS "FUNCIONES" DE LA TAREA, CREO

#MODIFICAR

TOPIC_NAME = "INFERENCE"

KAFKA_SERVER = "localhost:9092"

NOTIFICATION_TOPIC = "NOTIFICATION"
EMAIL_TOPIC = "EMAIL"

consumer = KafkaConsumer(
    TOPIC_NAME,
    # to deserialize kafka.producer.object into dict
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)

producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    api_version = (0, 11, 15)
)

def inferencProcessFunction(data):
	. . . . .
	. . . . .
	. . . . .
	# process steps
	. . . . .
	. . . . .
  
	notification_data = {...}
	email_data = {...}
    producer.send(NOTIFICATION_TOPIC, notification_data)
	producer.flush()
	producer.send(EMAIL_TOPIC, email_data)
	producer.flush()

for inf in consumer:
	
	inf_data = inf.value
  
    inferencProcessFunction(inf_data)