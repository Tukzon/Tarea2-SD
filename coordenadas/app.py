import asyncio
import json
import os
from kafka import KafkaConsumer
import time

async def consume():
    consumer = KafkaConsumer(
        'coordenadas',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',
        api_version=(0, 10, 1))
    await consumer.start()
    try:
        async for msg in consumer:
            print(msg)
            return json.loads(msg.value)
    finally:
        await consumer.stop()

async def coordenadas():
    data = "NO DATA"
    while True:
        try:
            data = await asyncio.wait_for(consume(), 60)
            print(data)
        except asyncio.TimeoutError:
            print('timeout')



if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(coordenadas())
    loop.run_forever()