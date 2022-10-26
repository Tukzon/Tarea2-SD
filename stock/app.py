import asyncio
import json
import os
from kafka import KafkaConsumer
#from .db.conn import query

async def consume():
    consumer = KafkaConsumer(
        'stock',
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

arr = []
async def stock():
    while len(arr) != 5:
        arr.append(await consume())
    print(arr)
    arr.clear()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(stock())
    loop.run_forever()