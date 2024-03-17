import pika, os
import pandas as pd
import json
import time

def productor():
    broker = 'amqps://jrupsjwm:hD4fTsj0iRW1gu-OlcV59kapI7Y0Cyvf@gull.rmq.cloudamqp.com/jrupsjwm'
    url = os.environ.get('CLOUDAMQP_URL', broker)
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    dataframe = pd.read_csv("DatosPruebaMQTT.csv", index_col=0)
    dataframe.head()
    dataframe.describe(include="all")

    temp = dataframe.Temperature.tolist()
    hum = dataframe.Humidity.tolist()
    co = dataframe.CO2.tolist()


    # Declaramos el intercambio si aún no existe. 'fanout' distribuye todos los mensajes a todas las colas vinculadas.
    intercambio = 'logs'
    channel.exchange_declare(exchange=intercambio, exchange_type='fanout')

    for i,j,k in zip(temp,hum,co):
        val1,val2,val3=json.dumps({"Temperatura":i}),json.dumps({"Humedad":j}),json.dumps({"Dioxido de carbono":k})
        print('Temperatura:',val1,'\n','Humedad:',val2,'\n','Dioxido de carbono:',val3)

        channel.basic_publish(exchange=intercambio, routing_key='', body=val1)
        channel.basic_publish(exchange=intercambio, routing_key='', body=val2)
        channel.basic_publish(exchange=intercambio, routing_key='', body=val3)

        time.sleep(1)
    print(f"Mensaje enviado: {mensaje}")

    connection.close()

productor()

