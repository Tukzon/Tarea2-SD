import schedule
import time
import threading
import os
import json
from kafka import KafkaConsumer
from db.conn import query


def add_dict_to_txt(dict):
    if not os.path.exists('./output/data.txt'):
        f = open('./output/data.txt', 'w')
        f.write(time.strftime("%d/%m/%Y") + '\n')
        f.write(str(dict))
        f.write('\n')
        f.write('-'*50)
        f.close()
    else:
        f = open('./output/data.txt', 'a')
        f.write(time.strftime("%d/%m/%Y") + '\n')
        f.write(str(dict))
        f.write('\n')
        f.write('-'*50)
        f.close()

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
    prom_ventas =  query("SELECT ventas.patente, sum(ventas.cantidad) AS suma, count(distinct ventas.cliente) AS cantidad FROM ventas WHERE data_time > now() - interval '1 day' GROUP BY patente")

    dic = {}
    for i in range(len(ventas)-1):
        dic[ventas[i][0]] = {'ventas': ventas[i][1], 'promedio_ventas': (prom_ventas[i][1]/prom_ventas[i][2]), 'clientes_totales': prom_ventas[i][2]}

    add_dict_to_txt(dic)

    return dic
    
    

schedule.every(1).days.do(ventas)

if __name__ == '__main__':
    while True:
        schedule.run_pending()
        time.sleep(1)