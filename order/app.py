import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
import threading
import redis
import requests
import pika
import json
import time
from queue import Queue
from pika.exceptions import AMQPConnectionError, ChannelClosedByBroker, StreamLostError
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)

response_queue_stock = Queue()
response_queue_payment = Queue()


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response

@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)

def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        message = json.dumps({'item_id': item_id, 'amount': quantity, 'action': 'rollback'})
        app.logger.info("Entered rollback")
        publish_message(message, "stock_queue")

@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    global response_queue_payment, response_queue_stock
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    # order_lock = db.lock("order_lock")
    # retries = 0
   
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    
    # The removed items will contain the items that we already have successfully subtracted stock from
    # for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    
    for item_id, quantity in items_quantities.items():
        message = json.dumps({'item_id': item_id, 'amount': quantity, 'action': 'subtract'})
        publish_message(message, "stock_queue")
        while response_queue_stock.empty():
            continue
        response = process_response(response_queue_stock.get())
        if response == 'insufficient stock':
            # If one item does not have enough stock we need to rollback
            rollback_stock(removed_items)
            abort(400, f'Out of stock on item_id: {item_id}')
        removed_items.append((item_id, quantity))

    message = json.dumps({'user_id': order_entry.user_id, 'amount': order_entry.total_cost, 'action': 'pay'})
    publish_message(message, "payment_queue")
    while response_queue_payment.empty():
        continue
    user_reply = process_response(response_queue_payment.get())

    app.logger.info("Received status from payment", user_reply )
    if user_reply == 'payment unsuccesfull':
        # If the user does not have enough credit we need to rollback all the item stock subtractions
        rollback_stock(removed_items)
        abort(400, f"User: {order_entry.user_id} out of credit")
    order_entry.paid = True
    
    # while retries < 3:
    #     if order_lock.acquire(blocking=False):  # Try to acquire the lock without blocking
    #         break
    #     retries += 1
    #     time.sleep(1)  # Wait for 1 second before retrying
    # else:
    #     # If we exit the while loop without breaking, it means all retries failed
    #     return abort(400, DB_ERROR_STR)
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    finally:
        # order_lock.release()
        app.logger.debug("Checkout successful")
        return Response("Checkout successful", status=200)

def process_response(response):
    if response['status'] == 200:
        app.logger.info("Response processed sucessfully")
        # Return the item details to the client
        return response['message']
    else:
        app.logger.error(f"Error: {response['message']}")
        # Handle the error appropriately
        return response['message']


def reconnect_rabbitmq():
    global connection, channel
    try:
        if connection and connection.is_open:
            connection.close()
        setup_rabbitmq()
    except pika.exceptions.AMQPConnectionError as e:
        app.logger.error(f"Failed to reconnect to RabbitMQ: {e}")

def publish_message(message, routing_key):
    app.logger.info("Order Service: Publishing Message")
    global connection
    channel = connection.channel()
        # Declare queues
    channel.queue_declare(queue="stock_queue")
    # channel.queue_declare(queue="stock_response_queue")
    channel.queue_declare(queue="payment_queue")
    try:
        if connection.is_closed or channel.is_closed:
            reconnect_rabbitmq()
        channel.basic_publish(
            exchange='',
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            ))
        app.logger.info("Order Service: Message Published")
    except (AMQPConnectionError, ChannelClosedByBroker, StreamLostError) as e:
        app.logger.error(f"Error publishing message: {e}")
        time.sleep(5)  # Delay before retry
        reconnect_rabbitmq()
        try:
            channel.basic_publish(
                exchange='',
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                ))
            app.logger.info("Order Service: Message Published after reconnection")
        except (AMQPConnectionError, ChannelClosedByBroker, StreamLostError) as e:
            app.logger.error(f"Failed to publish message after reconnection: {e}")

def on_response(channel, method_frame, header_frame, body):
    app.logger.info("Entered on response")
    msg = json.loads(body)
    if msg['origin'] ==  'stock':
        response_queue_stock.put(json.loads(body))
    else:
        response_queue_payment.put(json.loads(body))
    app.logger.info(f"channel: {channel}")
    return
    # channel.basic_ack(delivery_tag=method_frame.delivery_tag)


def consume_message_payment():
    # global connection
    connection_payment = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel_payment = connection_payment.channel()
    channel_payment.queue_declare(queue="payment_response_queue")
    app.logger.info("Consuming messages from RabbitMQ")
    try:
        consumer_tag = f"ctag-{uuid.uuid4()}"
        channel_payment.basic_consume(
            queue='payment_response_queue',
            on_message_callback=on_response,
            auto_ack=True,
            consumer_tag=consumer_tag,
        )
        app.logger.info("Starting RabbitMQ order consumer")
        channel_payment.start_consuming()
        app.logger.info("Finished consuming")
    except Exception as e:
        app.logger.error(f"Error in RabbitMQ order consumer: {e}")
        if connection_payment and connection_payment.is_open:
            connection_payment.close()
            

def consume_message_stock():
    # global connection
    connection_stock = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel_stock = connection_stock.channel()
    # 
    
    channel_stock.queue_declare(queue="stock_response_queue")
    app.logger.info("Consuming messages from RabbitMQ")
    app.logger.info(f"Channel stock: {channel_stock}")
    try:
        consumer_tag = f"ctag-{uuid.uuid4()}"
        channel_stock.basic_consume(
            queue='stock_response_queue',
            on_message_callback=on_response,
            auto_ack=True,
            consumer_tag=consumer_tag
        )
        app.logger.info("Starting RabbitMQ order consumer")
        channel_stock.start_consuming()
        app.logger.info("Finished consuming")
    except Exception as e:
        app.logger.error(f"Error in RabbitMQ order consumer: {e}")
        if connection_stock and connection_stock.is_open:
            connection_stock.close()


def setup_rabbitmq():
    app.logger.info("Setting up RabbitMQ connection")
    global connection
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
        # channel = connection.channel()
        # # Declare queues
        # channel.queue_declare(queue="stock_queue")
        # # channel.queue_declare(queue="stock_response_queue")
        # channel.queue_declare(queue="payment_queue")
        # channel.queue_declare(queue="payment_response_queue")
    except pika.exceptions.AMQPConnectionError as e:
        app.logger.error(f"Failed to connect to RabbitMQ: {e}")



with app.app_context():
    # Setup RabbitMQ connection and channel
    
    setup_rabbitmq()
    consumer_thread_payment = threading.Thread(target=consume_message_payment, daemon=True)
    consumer_thread_payment.start()
    consumer_thread_stock = threading.Thread(target=consume_message_stock, daemon=True)
    consumer_thread_stock.start()
    

if __name__ == "__main__":
    # Start Flask app in the main thread
    app.run(host="0.0.0.0", port=8000, debug=True)
    app.logger.info("Starting order service1")
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
