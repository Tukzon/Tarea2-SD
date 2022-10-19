from flask import Flask, request, jsonify
import json
from flask_cors import CORS
from kafka import KafkaConsumer, KafkaProducer

#kafka-topics.sh --create --bootstrap-server zookeeper:2181 --replication-factor 1 --partitions 1 --topic <topic_name>
#FUNCIÓN PARA CREAR LOS TOPICS, VER DIBUJO. INTEGRAR CON DOCKER COMPOSE

#kafka-console-producer.sh --broker-list host1:9092,host2:9092 --topic "topic_name"
#FUNCION PARA HACER EL PRODUCER(?

#kafka-console-consumer.sh --bootstrap-server <host_ip_of_producer>:9092 --topic "topic_name" --from-beginning
#FUNCION PARA HACER EL CONSUMER(?

app = Flask(__name__)
TOPIC_NAME = "INFERENCE"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    api_version = (0, 11, 15)
)

@app.route('/kafka/pushToConsumers', methods=['POST'])
def kafkaProducer():
		
    req = request.get_json()
    json_payload = json.dumps(req)
    json_payload = str.encode(json_payload)
		# push data into INFERENCE TOPIC
    producer.send(TOPIC_NAME, json_payload)
    producer.flush()
    print("Sent to consumer")
    return jsonify({
        "message": "You will receive an email in a short while with the plot", 
        "status": "Pass"})

if __name__ == "__main__":
    app.run(debug=True, port = 5000)