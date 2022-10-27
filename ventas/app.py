import schedule
import time
import asyncio
import json
from aiokafka import AIOKafkaConsumer
from db.conn import query

async def consume():
    consumer = AIOKafkaConsumer(
        'ventas',
        bootstrap_servers='kafka:9092')
    await consumer.start()
    try:
        async for msg in consumer:
            print(msg)
            return json.loads(msg.value)
    finally:
        await consumer.stop()

def ventas():
    ventas = query("SELECT patente, count(*) FROM ventas WHERE data_time > now() - interval '1 day' GROUP BY patente")
    prom_ventas =  query("SELECT avg(count) FROM (SELECT count(*) FROM ventas WHERE data_time > now() - interval '1 day' GROUP BY patente) as foo")
    clientes_totales = query("SELECT patente, count(distinct cliente) FROM ventas WHERE data_time > now() - interval '1 day' GROUP BY patente")
    #clientes_totales = query("SELECT count(*) FROM (SELECT DISTINCT cliente FROM ventas WHERE data_time > now() - interval '1 day' GROUP BY patente) as foo")

    dic = {}
    for i in range(len(ventas)-1):
        dic[ventas[i][0]]["ventas"] = ventas[i][1]
        dic[ventas[i][0]]["promedio_ventas"] = prom_ventas[i][0]
        dic[ventas[i][0]]["clientes_totales"] = clientes_totales[i][0]

    print(dic)
    
    

schedule.every(1).minutes.do(ventas)

if __name__ == '__main__':
    while True:
        schedule.run_pending()
        time.sleep(1)