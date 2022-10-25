from crypt import methods
import json
from time import time
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import asyncio

app = Flask(__name__)
topic_list = []

def serializer(message):
    return json.dumps(message).encode('utf-8')

async def send_one(message):
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=serializer)
    await producer.start()
    try:
        await producer.send_and_wait("test", message)
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
    return jsonify(data), 201

@app.route('/newVenta', methods=['POST'])
def newVenta():
    data = request.get_json()
    if not data:
        return jsonify({'message': 'No input data provided'}), 400
    return jsonify(data), 201

@app.route('/carritoProfugo', methods=['POST'])
def carritoProfugo():
    data = request.get_json()
    if not data:
        return jsonify({'message': 'No input data provided'}), 400
    return jsonify(data), 201

if __name__== "__main__":
    app.run(host='0.0.0.0' ,debug = True,port = 8000)