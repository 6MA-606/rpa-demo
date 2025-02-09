import pika

rabbitmq_host = "zyxmanetwork.thddns.net"
rabbitmq_port = 5051

connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))

channel = connection.channel()

channel.exchange_declare(exchange="documents", exchange_type="topic")

result = channel.queue_declare(queue='', exclusive=True)
quene_name = result.method.queue

channel.queue_bind(exchange="documents", queue=quene_name, routing_key="documents.#")

def callback(ch, method, properties, body):
    print(f" [x] Received {body}")

channel.basic_consume(queue=quene_name, on_message_callback=callback, auto_ack=True)

print(" [*] Waiting for messages. To exit press CTRL+C")

channel.start_consuming()