import logging
import os
import atexit
import random
import uuid
from collections import defaultdict

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, AIOKafkaClient
from quart import Quart, request, jsonify
import json

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

KAFKA_SERVER = os.environ.get('KAFKA_SERVER', 'kafka:9092')
# ORDER_TOPIC = os.environ.get('ORDER_TOPIC', 'order_topic')
# PAYMENT_TOPIC = os.environ.get('PAYMENT_TOPIC', 'payment_topic')
# STOCK_TOPIC = os.environ.get('STOCK_TOPIC', 'stock_topic')
# GROUP_ID = os.environ.get('GROUP_ID', 'order_service')


app = Quart("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

producer = None
consumer = None

async def init_kafka_producer():
    app.logger.info("Initializing producer")
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers='kafka:9092',
        # enable_idempotence=True,
        # transactional_id="order-service-transactional-id",
        # api_version=(0,11,5)
    )
    app.logger.info("Producer initialized")
    await producer.start()
    app.logger.info("Await complete")

async def init_kafka_consumer():
    app.logger.info("Initializing consumer")
    global consumer
    consumer = AIOKafkaConsumer(
        'stock_response',
        bootstrap_servers='kafka:9092',
        group_id="my-group")
    # Get cluster layout and join group `my-group`
    await consumer.start()

async def stop_kafka_producer():
    await producer.stop()

async def stop_kafka_consumer():
    await consumer.stop

async def consume():
    app.logger.info("Entered consume")
    
    try:
        app.logger.info("Entered try consume")
        # Consume messages
        async for msg in consumer:
            app.logger.info(f"entered order consume ")
            stock_data = msgpack.decode(msg.value)
            action = stock_data['action']
            app.logger.info(f"{stock_data}")
            if action == 'find':
                app.logger.info("action is find")
                return stock_data
                # item_entry = await find_item(item_id)

    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

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
    print("Creating order")
    app.logger.info("Creating order")
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
    app.logger.info("entered find order")
    print("entered find order")
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

@app.get('/find_item/<item_id>')
async def find_item(item_id: str):
    try:
        app.logger.info("Entered find_item")
        message = {'action': 'find', 'item_id': item_id}
        # async with producer.transaction():
        app.logger.info("Starting send_and_wait")
        await producer.send_and_wait('stock_request', value=msgpack.encode(message))
        response = await consume()
        app.logger.info("Response from consume")
        if response['action'] == 'find':
            if response['status'] == '400':
                return abort(400, response['msg'])
            else:
                return jsonify(response['msg'])
        else:
            return abort(400, "Unexpected response from stock service")
    except Exception as e:
        app.logger.info(f"error:{str(e)}" )
        return abort(400, f"Error while finding item: {str(e)}")

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
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    # The removed items will contain the items that we already have successfully subtracted stock from
    # for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
        if stock_reply.status_code != 200:
            # If one item does not have enough stock we need to rollback
            rollback_stock(removed_items)
            abort(400, f'Out of stock on item_id: {item_id}')
        removed_items.append((item_id, quantity))
    user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
    if user_reply.status_code != 200:
        # If the user does not have enough credit we need to rollback all the item stock subtractions
        rollback_stock(removed_items)
        abort(400, "User out of credit")
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)
    
async def main():
    await init_kafka_producer()
    app.logger.info("Completed Initialization")
    await init_kafka_consumer()
    app.logger.info("Completed consumer")
    # await asyncio.Event().wait() 
    # await asyncio.Event().wait()  # Keep the service running


@app.before_serving
async def run_main():
    await main()


if __name__ == '__main__':
    # asyncio.run(main())
    app.logger.info("Entering main")
    app.run(host="0.0.0.0", port=8000, debug=True, use_reloader=True)
    # asyncio.run(main())
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
