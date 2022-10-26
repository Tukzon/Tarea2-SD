from crypt import methods
import json
from time import time
from flask import Flask, request, jsonify
from aiokafka import AIOKafkaProducer
import psycopg2
import asyncio

# CONFIGURE POSTGRES CONNECTION
conn = psycopg2.connect(
    host="postgres",
    database="tarea2",
    user="postgres",
    password="postgres")

cursor = conn.cursor()

app = Flask(__name__)
topic_list = []

def serializer(data):
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
        await producer.stop()

@app.route('/')
def index():
        return jsonify({'message': 'Hello World!'})

@app.route('/newMember', methods=['POST'])
def NewMember():
    data = request.get_json()
    if not data:
        return jsonify({'message': 'No input data provided'}), 400
    app.logger.info(data)
    asyncio.run(send_one(data, 'miembros'))
    return jsonify({'message': 'Waiting for consumer!'}), 200

@app.route('/newVenta', methods=['POST'])
def newVenta():
    data = request.get_json()
    if not data:
        return jsonify({'message': 'No input data provided'}), 400
    cursor.execute("INSERT INTO ventas (patente,cliente,cantidad,ubicacion) VALUES (%s,%s,%s,%s)", (data['patente'],data['cliente'],data['cantidad'],data['ubicacion']))
    conn.commit()
    return jsonify({'message': 'Venta created successfully'}), 201

@app.route('/carritoProfugo', methods=['POST'])
def carritoProfugo():
    data = request.get_json()
    if not data:
        return jsonify({'message': 'No input data provided'}), 400
    cursor.execute("INSERT INTO carritos (coordenadas) VALUES (%s)", (data['coordenadas'],))
    return jsonify(data), 201

if __name__== "__main__":
    app.run(host='0.0.0.0' ,debug = True,port = 8000)