import pika,os

broker = 'amqps://jrupsjwm:hD4fTsj0iRW1gu-OlcV59kapI7Y0Cyvf@gull.rmq.cloudamqp.com/jrupsjwm'

def receptor():
    url = os.environ.get('CLOUDAMQP_URL', broker)
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Solicitar que colas se van a consumir
    colas=["temp","humid","CO2"]

    # Seleccion de que cola se va a consumir con codigo 0, 1, 2
    print("Seleccione las colas a consumir: ")
    print("0.- Temperatura")
    print("1.- Humedad")
    print("2.- CO2")
    print("3.- Todas")
    print("4.- Salir")
    seleccion = int(input("Opción: "))
    if seleccion == 0:
        colas=["temp"]
    elif seleccion == 1:
        colas=["humid"]
    elif seleccion == 2:
        colas=["CO2"]
    elif seleccion == 3:
        colas=["temp","humid","CO2"]
    elif seleccion == 4:
        print("Saliendo del programa")
        exit()
    else:
        print("Opción no valida")
        exit()

    for i in colas:
        channel.queue_declare(queue=i)
        
    print("En espera de mensajes")
    
    def callback(ch, method, properties, body):
        print("Mensaje recibido %r" % body.decode())
        # Confirmamos la recepción y procesamiento del mensaje
        ch.basic_ack(delivery_tag=method.delivery_tag)

    for j in colas:
        print(f"Consumiendo de la cola {j}")
        channel.basic_consume(queue=j,on_message_callback=callback)
        print()
    
    channel.start_consuming()
    
try:
    receptor()
except KeyboardInterrupt: #cuando presionas Ctrl + C
    print("Recepción de datos detenida por el usuario")
