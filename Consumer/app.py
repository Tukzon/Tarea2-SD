import json
from flask import Flask, redirect, jsonify
from kafka import KafkaConsumer
import asyncio

app = Flask(__name__)
topics = []
message = ''

async def consume():
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest')
    await consumer.start()
    try:
        async for msg in consumer:
            print(msg)
            return json.loads(msg.value)
    finally:
        await consumer.stop()

@app.route('/')
def index():
    return jsonify({'message': 'Hello World!'})
    
if __name__== "__main__":
    app.run(host='0.0.0.0',debug = True,port = 5000)