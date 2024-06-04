from flask import Flask, request, jsonify
import pika
import threading
import json
import requests
from queue import Queue
import logging

app = Flask("order-consumer-service")

response_queues = {
    "stock": Queue(),
    "payment": Queue()
}

def setup_rabbitmq():
    global connection, channel
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()
    channel = connection.channel()
    # channel.queue_declare(queue="stock_queue")
    # channel.queue_declare(queue="order_queue")
    # channel.queue_declare(queue="payment_queue")
    channel.queue_declare(queue="order_response_queue")

def on_response(channel, method, properties, body):
    app.logger.info("Received response")
    response = json.loads(body)
    if response["origin"] == "stock":
        response_queues["stock"].put(response)
    elif response["origin"] == "payment":
        response_queues["payment"].put(response)
    channel.basic_ack(delivery_tag=method.delivery_tag)

def consume_messages():
    app.logger.info("Consuming payment messages")
    channel.basic_consume(queue="order_response_queue", on_message_callback=on_response, auto_ack=False)
    channel.start_consuming()

@app.get('/consume/<queue_type>')
def consume_message(queue_type: str):
    app.logger.info("I AM IN THE CONSUMER SERVICE")
    try:
        response = response_queues[queue_type].get(timeout=3)  # Adjust timeout as needed
        return jsonify(response), 200
    except Exception as e:
        if response_queues[queue_type].empty():
            app.logger.info("No messages in queue")
        return jsonify({"error": "No messages in queue"}), 400
    

with app.app_context():
    app.logger.info("Setting up RabbitMQ connection")
    setup_rabbitmq()
    consume_messages()

if __name__ == "__main__":
    # Start Flask app in the main thread
    app.run(host="0.0.0.0", port=8000, debug=True)
    app.logger.info("Starting stock service1")
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    
# response_queues = {
#     "stock": Queue(),
#     "payment": Queue()
# }

    
# def on_stock_response(channel, method, properties, body):
#     app.logger.info("Received stock response")
#     response = json.loads(body)
#     response_queue["stock"].put(response)
#     channel.basic_ack(delivery_tag=method.delivery_tag)

# def on_payment_response(channel, method, properties, body):
#     app.logger.info("Received payment response")
#     response = json.loads(body)
#     response_queue["payment"].put(response)
#     channel.basic_ack(delivery_tag=method.delivery_tag)

# def consume_stock_messages():
#     app.logger.info("Consuming stock messages")
#     channel.basic_consume(queue="stock_response_queue", on_message_callback=on_stock_response, auto_ack=True)
#     channel.start_consuming()

# def consume_payment_messages():
#     app.logger.info("Consuming payment messages")
#     channel.basic_consume(queue="payment_response_queue", on_message_callback=on_payment_response, auto_ack=True)
#     channel.start_consuming()

# if __name__ == "__main__":
#     setup_rabbitmq()
#     threading.Thread(target=consume_stock_messages).start()
#     threading.Thread(target=consume_payment_messages).start()
#     app.run(host="0.0.0.0", port=8000, debug=True)