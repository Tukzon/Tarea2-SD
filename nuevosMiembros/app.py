import threading, logging
import json
import os
from kafka import KafkaConsumer
import time
from db.conn import query

def add_dict_to_txt(dict):
    if not os.path.exists('./output/data.txt'):
        f = open('./output/data.txt', 'w')
        f.write(str(dict))
        f.write('\n')
        f.write('-'*50)
        f.close()
    else:
        f = open('./output/data.txt', 'a')
        f.write(str(dict))
        f.write('\n')
        f.write('-'*50)
        f.close()

def bytes_to_array(bytes):
    return json.loads(bytes.decode('utf-8'))

class Consumer(threading.Thread):

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='kafka:9092',
                                auto_offset_reset='earliest')
        consumer.subscribe(['miembros'])
        self.valid = 0
        self.invalid = 0
        self.msg = None

        for message in consumer:
            if message.value != None:
                self.valid += 1
                self.msg = bytes_to_array(message.value)

            else:
                self.invalid += 1
                consumer_stop.set()
                break

            if consumer_stop.is_set():
                self.msg = None
                break
    
            consumer.close()

def main():
    thread = Consumer()
    thread.start()

    while True:
        time.sleep(1)
        nuevoMiembro(thread.msg)
        if thread.msg != None:
            add_dict_to_txt(thread.msg)
            thread.msg = None

'''def deserialize(data):
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
'''
def nuevoMiembro(data):
    #if query(f"SELECT * FROM miembros WHERE rut = '{data['rut']}'") != None:
    #    add_dict_to_txt("Miembro ya existe")
    #    return "ERROR: Miembro ya existe - rut: " + data['rut']
    consulta = f"INSERT INTO miembros (nombre,apellido,rut,correo,patente,premium,stock_inicial) VALUES ('{data['nombre']}', '{data['apellido']}', '{data['rut']}', '{data['correo']}', '{data['patente']}', {data['premium']}, {data['stock_inicial']})"
    query(consulta)
    add_dict_to_txt(data['nombre'] + " " + data['apellido'] + " ha sido agregado a la base de datos")
    return data



if __name__ == '__main__':
    #loop = asyncio.get_event_loop()
    #loop.create_task(nuevoMiembro())
    #loop.run_forever()
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()