import json
import time, threading, logging
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import psycopg2

# CONFIGURE POSTGRES CONNECTION
conn = psycopg2.connect(
    host="postgres",
    database="tarea2",
    user="postgres",
    password="postgres")

cursor = conn.cursor()
producer_stop = threading.Event()

app = Flask(__name__)

# array to bytes function
def array_to_bytes(arr):
    return bytes(json.dumps(arr), encoding='utf-8')

class Producer(threading.Thread):
    msg = None

    def run(self, topic, data, partit=0):
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        self.sent = 0
        self.msg = array_to_bytes(data)

        while not producer_stop.is_set():
            producer.send(topic, self.msg, partition=partit)
            self.sent += 1
            producer_stop.set()
        producer.flush()

'''def serializer(data):
    return json.dumps(data).encode('utf-8')

async def send_one(message, topic):
    producer = AIOKafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=serializer
        )
    await producer.start()
    try:
        await producer.send_and_wait(topic, message)
    finally:
        await producer.stop()'''

@app.route('/')
def index():
        return jsonify({'message': 'Hello World!'})

@app.route('/newMember', methods=['POST'])
def NewMember():
    data = request.get_json()
    if not data:
        return jsonify({'message': 'No input data provided'}), 400
    app.logger.info(data)
    #asyncio.run(send_one(data, 'miembros'))
    return jsonify({'message': 'Waiting for consumer!'}), 200

@app.route('/newVenta', methods=['POST'])
def newVenta():
    data = request.get_json()
    if not data:
        return jsonify({'message': 'No input data provided'}), 400
    qty = cursor.execute("SELECT stock FROM miembros WHERE patente = %s", (data['patente']))
    qtyfetch = cursor.fetchone()
    if data['cantidad'] <= qtyfetch[0]:
        cursor.execute("INSERT INTO ventas (patente,cliente,cantidad,ubicacion) VALUES (%s,%s,%s,%s)", (data['patente'],data['cliente'],data['cantidad'],data['ubicacion']))
        cursor.execute("UPDATE miembros SET stock = stock - %s WHERE patente = %s", (data['cantidad'],data['patente']))
        conn.commit()
        return jsonify({'message': 'Venta created successfully'}), 201
    else:
        return jsonify({'message': 'Not enough stock'}), 400

@app.route('/carritoProfugo', methods=['POST'])
def carritoProfugo():
    data = request.get_json()
    if not data:
        return jsonify({'message': 'No input data provided'}), 400
    app.logger.info("Sending to Consumer")
    Producer().run('coordenadas', data['coordenadas'], 0)
    app.logger.info('Message sent')
    #asyncio.run(send_one(data['coordenadas'], 'coordenadas'))
    #cursor.execute("INSERT INTO carritos (coordenadas) VALUES (%s)", (data['coordenadas'],))
    return jsonify(data, "OK"), 201


if __name__== "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    app.run(host='0.0.0.0' ,debug = True,port = 8000, threaded=True)