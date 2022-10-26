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

def serializer(message, topic):
    return json.dumps(message).encode('utf-8')

async def send_one(message):
    producer = AIOKafkaProducer(
        bootstrap_servers='kafka:9092'
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
    for i in data:
        cursor.execute("INSERT INTO miembros (nombre,apellido,rut,correo,patente,premium,stock_inicial) VALUES (%(str)s,%(str)s,%(str)s,%(str)s,%(bool)s,%(int)s)", (i[0],i[1],i[2],i[3],i[4],bool(i[5]),int(i[6])))

    #if not data:
     #   return jsonify({'message': 'No input data provided'}), 400
    #if cursor.execute("SELECT * FROM miembros WHERE rut = %s", (data['rut'],)) != None:
    #    return jsonify({'message': 'Member already exists'}), 400
    #cursor.execute("INSERT INTO miembros (nombre,apellido,rut,correo,patente,premium,stock_inicial) VALUES (%(str)s,%(str)s,%(str)s,%(str)s,%(bool)s,%(int)s)", (data['nombre'],data['apellido'],data['rut'],data['correo'],data['patente'],bool(data['premium']),int(data['stock_inicial'])))
    conn.commit()
    return jsonify({'message': 'Member created successfully'}), 201

@app.route('/newVenta', methods=['POST'])
def newVenta():
    data = request.get_json()['data']
    if not data:
        return jsonify({'message': 'No input data provided'}), 400
    cursor.execute("INSERT INTO ventas (patente,cliente,cantidad,ubicacion) VALUES (%(str)s,%(str)s,%(int)s,%(str)s)", (data['patente'],data['cliente'],data['cantidad'],data['ubicacion']))
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