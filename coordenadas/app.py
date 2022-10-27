import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer
import time

def add_value_to_file(value):
    if not os.path.exists('./output/data.txt'):
        f = open('./output/data.txt', 'w')
        f.write(value+'\n')
        f.close()
    else:
        f = open('./output/data.txt', 'a')
        f.write(value+'\n')
        f.close()


async def consume():
    consumer = AIOKafkaConsumer(
        'coordenadas',
        bootstrap_servers='kafka:9092')
    await consumer.start()
    try:
        async for msg in consumer:
            print(msg)
            return json.loads(msg.value)
    finally:
        await consumer.stop()

async def coordenadas():
    while True:
        try:
            data = await asyncio.wait_for(consume(), 60)
            add_value_to_file(str(data))
            print(data)
        except asyncio.TimeoutError:
            print('timeout')



if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(coordenadas())
    loop.run_forever()