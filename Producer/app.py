from crypt import methods
import json
from time import time
from flask import Flask, render_template, request
from aiokafka import AIOKafkaProducer
import asyncio

app = Flask(__name__)
topic_list = []


async def send_one(message):
    producer = AIOKafkaProducer(
        bootstrap_servers='kafka:9092')
    await producer.start()
    try:
        await producer.send_and_wait("test", message)
    finally:
        await producer.stop()

@app.route('/')
def index():
        return render_template('index.html')

@app.route('/login', methods=['POST'])
def login():
    if request.method == 'POST':
        user = request.form['user']
        message={'user': user, 'time_login': time()}
        message = json.dumps(message).encode('utf-8')
        asyncio.run(send_one(message))
    return render_template('index.html')

@app.route('/newMember', methods=['POST'])
def NewMember():
    if request.method == 'POST':
        user = request.form['user']
        message={'user': user, 'time_register': time()}
        message = json.dumps(message).encode('utf-8')
        asyncio.run(send_one(message))
    return render_template('index.html')

@app.route('/deleteMember', methods=['DELETE'])
def DeleteMember():
    if request.method == 'DELETE':
        user = request.form['user']
        message={'user': user, 'time_delete': time()}
        message = json.dumps(message).encode('utf-8')
        asyncio.run(send_one(message))
    return render_template('index.html')

@app.route('/carritoProfugo', methods=['POST'])
def carritoProfugo():
    print("uwu")

@app.route('/carritoPosicion', methods=['PATCH'])
def carritoPosicion():
    print("owo")

@app.route('/newVenta', methods=['POST'])
def newVenta():
    print("iwi")

@app.route('/deleteVenta', methods=['DELETE'])
def deleteVenta():
    print("awa")   

if __name__== "__main__":
    app.run(debug = True,port = 8000)