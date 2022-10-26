from crypt import methods
import json
from time import time
from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.errors import KafkaError
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

def serializer(message):
    return json.dumps(message).encode('utf-8')

async def send_one(message):
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=serializer,
        api_version=(0, 10, 1)
    )
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
    if cursor.execute("SELECT * FROM miembros WHERE rut = %s", (data['rut'],)).fetchone():
        return jsonify({'message': 'Member already exists'}), 400
    cursor.execute("INSERT INTO miembros (nombre,apellido,rut,correo,patente,premium) VALUES (%s,%s,%s,%s,%s,%s)", (data['nombre'],data['appelido'],data['rut'],data['correo'],data['patente'],data['premium']))
    conn.commit()
    return jsonify({'message': 'Member created successfully'}), 201

@app.route('/newVenta', methods=['POST'])
def newVenta():
    data = request.get_json()
    if not data:
        return jsonify({'message': 'No input data provided'}), 400
    cursor.execute("INSERT INTO ventas (cliente,cantidad,hora,stock,ubicacion) VALUES (%s,%s,%s,%s,%s)", (data['cliente'],data['cantidad'],data['hora'],data['stock'],data['ubicacion']))
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