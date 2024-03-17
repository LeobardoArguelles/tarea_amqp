import pika
import pandas as pd
import time
import os
import json

dataframe = pd.read_csv("DatosPruebaMQTT.csv", index_col=0)
dataframe.head()
dataframe.describe(include="all")

temp = dataframe.Temperature.tolist()
hum = dataframe.Humidity.tolist()
co = dataframe.CO2.tolist()

broker = 'amqps://jrupsjwm:hD4fTsj0iRW1gu-OlcV59kapI7Y0Cyvf@gull.rmq.cloudamqp.com/jrupsjwm'

url = os.environ.get('CLOUDAMQP_URL', broker)
params = pika.URLParameters(url)
connect = pika.BlockingConnection(params)
channel = connect.channel()

colas=["temp","humid","CO2"]
for q in colas:
    channel.queue_declare(queue=q)
        
time.sleep(0.1)
try:
    for i,j,k in zip(temp,hum,co):
        val1,val2,val3=json.dumps({"Temperatura":i}),json.dumps({"Humedad":j}),json.dumps({"Dioxido de carbono":k})
        print(colas[0],val1,'\n',colas[1],val2,'\n',colas[2],val3)

        channel.basic_publish(exchange='', routing_key='temp', body=val1)
        channel.basic_publish(exchange='', routing_key='humid', body=val2)
        channel.basic_publish(exchange='', routing_key='CO2', body=val3)
        time.sleep(1)
except KeyboardInterrupt:
    print("Envío de datos detenido por el usuario")
    connect.close()
