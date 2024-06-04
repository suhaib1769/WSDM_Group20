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
    global connection1, connection2, channel1, channel2
    connection1 = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    connection2 = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel1 = connection1.channel()
    channel2 = connection2.channel()
    channel1.queue_declare(queue="stock_response_queue")
    channel2.queue_declare(queue="payment_response_queue")

def on_response(channel, method, properties, body):
    app.logger.info("Received response")
    response = json.loads(body)
    app.logger.info(response)
    if response["origin"] == "stock":
        response_queues["stock"].put(response)
        app.logger.info("Stock response queue: ",response_queues["stock"])
    elif response["origin"] == "payment":
        response_queues["payment"].put(response)
        app.logger.info("Payment response queue: ",response_queues["payment"])
    channel.basic_ack(delivery_tag=method.delivery_tag)

def consume_stock_messages():
    global connection1, channel1
    app.logger.info("Consuming messages from RabbitMQ")
    try:
        channel1.basic_consume(
            queue="stock_response_queue",
            on_message_callback=on_response,
            # add_callback_threadsafe=on_response,
            auto_ack=False,
        )
        app.logger.info("Starting RabbitMQ stock consumer")
        channel1.start_consuming()
    except Exception as e:
        app.logger.error(f"Error in RabbitMQ stock consumer: {e}")
        if connection1 and connection1.is_open:
            connection1.close()

def consume_payment_messages():
    global connection2, channel2
    app.logger.info("Consuming messages from RabbitMQ")
    try:
        channel2.basic_consume(
            queue="payment_response_queue",
            on_message_callback=on_response,
            auto_ack=False,
        )
        app.logger.info("Starting RabbitMQ payment consumer")
        channel2.start_consuming()
    except Exception as e:
        app.logger.error(f"Error in RabbitMQ payment consumer: {e}")
        if connection2 and connection2.is_open:
            connection2.close()

@app.get('/consume/<queue_type>')
def consume_message(queue_type: str):
    response = None
    app.logger.info(f"I AM IN THE CONSUMER SERVICE {queue_type}")
    try:
        response = response_queues[queue_type].get()  # Adjust timeout as needed
        return jsonify(response), 200
    except Exception as e:
        if response_queues[queue_type].empty():
            app.logger.info("No messages in queue")
        return response
    

with app.app_context():
    app.logger.info("Setting up RabbitMQ connection")
    setup_rabbitmq()
    # Start RabbitMQ stock consumer in a separate thread
    stock_consumer_thread = threading.Thread(target=consume_stock_messages)
    stock_consumer_thread.start()
    # Start RabbitMQ payment consumer in a separate thread
    payment_consumer_thread = threading.Thread(target=consume_payment_messages)
    payment_consumer_thread.start()

if __name__ == "__main__":
    # Start Flask app in the main thread
    app.run(host="0.0.0.0", port=8000, debug=True)
    app.logger.info("Starting stock service1")
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
