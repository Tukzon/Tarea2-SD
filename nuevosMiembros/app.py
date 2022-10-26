import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer
import time
from db.conn import query
from flask import Flask, request, jsonify

app = Flask(__name__)

def deserialize(data):
    return json.loads(data.decode('utf-8'))

async def consume():
    consumer = AIOKafkaConsumer(
        'miembros',
        bootstrap_servers='kafka:9092')
    await consumer.start()
    try:
        async for msg in consumer:
            print(msg)
            return json.loads(msg.value)
    finally:
        await consumer.stop()

async def nuevoMiembro():
    data = await consume()
    app.logger.info(data)
    if query(f"SELECT * FROM miembros WHERE rut = '{data['rut']}'") != None:
        app.logger.info("Miembro ya existe")
        return "ERROR: Miembro ya existe - rut: " + data['rut']
    consulta = f"INSERT INTO miembros (nombre,apellido,rut,correo,patente,premium,stock_inicial) VALUES ('{data['nombre']}', '{data['apellido']}', '{data['rut']}', '{data['correo']}', '{data['patente']}', {data['premium']}, {data['stock_inicial']})"
    query(consulta)
    app.logger.info(data['nombre'] + " " + data['apellido'] + " ha sido agregado a la base de datos")
    return data



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
    loop = asyncio.get_event_loop()
    loop.create_task(nuevoMiembro())
    loop.run_forever()