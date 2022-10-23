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
    if request.method == 'POST':
        user = request.form['user']
        message={'user': user, 'time_register': time()}
        message = json.dumps(message).encode('utf-8')
        asyncio.run(send_one(message))
    return jsonify({'message': message})

@app.route('/deleteMember', methods=['DELETE'])
def DeleteMember():
    if request.method == 'DELETE':
        user = request.form['user']
        message={'user': user, 'time_delete': time()}
        message = json.dumps(message).encode('utf-8')
        asyncio.run(send_one(message))
    return jsonify({'message': 'Hello World!'})

@app.route('/carritoProfugo', methods=['POST'])
def carritoProfugo():
    return jsonify({'message': 'Hello World!'})

@app.route('/carritoPosicion', methods=['PATCH'])
def carritoPosicion():
    return jsonify({'message': 'Hello World!'})

@app.route('/newVenta', methods=['POST'])
def newVenta():
    return jsonify({'message': 'Hello World!'})

@app.route('/deleteVenta', methods=['DELETE'])
def deleteVenta():
    return jsonify({'message': 'Hello World!'})  

if __name__== "__main__":
    app.run(host='0.0.0.0' ,debug = True,port = 8000)