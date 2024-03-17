import pika, os

def receptor():
    broker = 'amqps://jrupsjwm:hD4fTsj0iRW1gu-OlcV59kapI7Y0Cyvf@gull.rmq.cloudamqp.com/jrupsjwm'
    url = os.environ.get('CLOUDAMQP_URL', broker)
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Declaramos un intercambio de tipo 'topic', 'direct', o 'fanout'. 'fanout' es típico para pub/sub
    intercambio = 'logs'
    channel.exchange_declare(exchange=intercambio, exchange_type='fanout')

    # Creamos una cola temporal exclusiva para este consumidor
    resultado = channel.queue_declare(queue='', exclusive=True)
    nombre_cola = resultado.method.queue

    # Vinculamos la cola al intercambio
    channel.queue_bind(exchange=intercambio, queue=nombre_cola)

    print('En espera de mensajes. Para salir presiona CTRL+C')

    def callback(ch, method, properties, body):
        print(f"Mensaje recibido: {body.decode()}")

    channel.basic_consume(queue=nombre_cola, on_message_callback=callback, auto_ack=True)

    channel.start_consuming()

try:
    receptor()
except KeyboardInterrupt:
    print("Recepción de datos detenida por el usuario")

