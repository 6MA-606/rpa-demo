import pika
from load_dotenv import env

connection = None
channel = None
rabbitmq_host = env["RABBITMQ_HOST"]
rabbitmq_port = env["RABBITMQ_PORT"]

def get_rabbitmq_connection():

    global connection

    if connection:
        return connection

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))
        print("[x] RabbitMQ connection established!")
        return connection
    except Exception as e:
        print(f"[x] RabbitMQ connection error: {e}")
        return None

def get_rabbitmq_channel():

    global connection
    global channel

    if channel:
        return channel

    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        print("[x] RabbitMQ channel established!")
        return channel
    except Exception as e:
        print(f"[x] RabbitMQ channel error: {e}")
        return None
    
def close_rabbitmq_connection():
    global connection
    global channel

    if connection:
        connection.close()
        channel = None
        connection = None

def set_rabbitmq_queue(queue_name):
    channel = get_rabbitmq_channel()
    channel.queue_declare(queue=queue_name, durable=True)

def set_rabbitmq_exchange(exchange_name, exchange_type):
    channel = get_rabbitmq_channel()
    channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=True)

def publish_message(queue_name, message):
    channel = get_rabbitmq_channel()
    channel.basic_publish(exchange="",
                          routing_key=queue_name,
                          body=message,
                          properties=pika.BasicProperties(
                              delivery_mode=pika.DeliveryMode.Persistent
                         ))

def publish_message_to_exchange(exchange_name, exchange_type, routing_key, message):
    channel = get_rabbitmq_channel()

    channel.exchange_declare(exchange=exchange_name,
                             exchange_type=exchange_type,
                             durable=True)

    channel.basic_publish(exchange=exchange_name,
                          routing_key=routing_key,
                          body=message,
                          properties=pika.BasicProperties(
                              delivery_mode=pika.DeliveryMode.Persistent
                          ))
    
def consume_message(queue_name, callback):
    channel = get_rabbitmq_channel()
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    print(f"Consuming messages from {queue_name}")
    channel.start_consuming()