# import logging
# import os
# import atexit
# import random
# import uuid
# from collections import defaultdict

# import redis
# import requests

# from msgspec import msgpack, Struct
# from flask import Flask, jsonify, abort, Response

# import asyncio
# from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, AIOKafkaClient


# DB_ERROR_STR = "DB error"
# REQ_ERROR_STR = "Requests error"

# GATEWAY_URL = os.environ['GATEWAY_URL']

# KAFKA_SERVER = os.environ.get('KAFKA_SERVER', 'localhost:9092')
# # ORDER_TOPIC = os.environ.get('ORDER_TOPIC', 'order_topic')
# # PAYMENT_TOPIC = os.environ.get('PAYMENT_TOPIC', 'payment_topic')
# # STOCK_TOPIC = os.environ.get('STOCK_TOPIC', 'stock_topic')
# # GROUP_ID = os.environ.get('GROUP_ID', 'order_service')


# app = Flask("order-service")

# db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
#                               port=int(os.environ['REDIS_PORT']),
#                               password=os.environ['REDIS_PASSWORD'],
#                               db=int(os.environ['REDIS_DB']))


# def close_db_connection():
#     db.close()


# atexit.register(close_db_connection)


# class OrderValue(Struct):
#     paid: bool
#     items: list[tuple[str, int]]
#     user_id: str
#     total_cost: int

# producer = None

# async def init_kafka_producer():
#     app.logger.info("Initializing kafka producer")
#     global producer
#     producer = AIOKafkaProducer(
#         bootstrap_servers=KAFKA_SERVER,
#         enable_idempotence=True,
#         transactional_id="order-service-transactional-id"
#     )
#     await producer.start()

# async def stop_kafka_producer():
#     await producer.stop()

# async def consume():
#     consumer = AIOKafkaConsumer(
#         'stock_response',
#         bootstrap_servers='localhost:9092',
#         group_id="my-group")
#     # Get cluster layout and join group `my-group`
#     await consumer.start()
#     try:
#         # Consume messages
#         async for msg in consumer:
#             stock_data = msgpack.decode(msg.value)
#             action = stock_data.action
#             if action == 'find':
#                 item_id = stock_data.item_id
#                 item_entry = find_item(item_id)

#     finally:
#         # Will leave consumer group; perform autocommit if enabled.
#         await consumer.stop()

# def get_order_from_db(order_id: str) -> OrderValue | None:
#     try:
#         # get serialized data
#         entry: bytes = db.get(order_id)
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     # deserialize data if it exists else return null
#     entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
#     if entry is None:
#         # if order does not exist in the database; abort
#         abort(400, f"Order: {order_id} not found!")
#     return entry


# @app.post('/create/<user_id>')
# def create_order(user_id: str):
#     print("Creating order")
#     app.logger.info("Creating order")
#     key = str(uuid.uuid4())
#     value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
#     try:
#         db.set(key, value)
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     return jsonify({'order_id': key})


# @app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
# def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

#     n = int(n)
#     n_items = int(n_items)
#     n_users = int(n_users)
#     item_price = int(item_price)

#     def generate_entry() -> OrderValue:
#         user_id = random.randint(0, n_users - 1)
#         item1_id = random.randint(0, n_items - 1)
#         item2_id = random.randint(0, n_items - 1)
#         value = OrderValue(paid=False,
#                            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
#                            user_id=f"{user_id}",
#                            total_cost=2*item_price)
#         return value

#     kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
#                                   for i in range(n)}
#     try:
#         db.mset(kv_pairs)
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     return jsonify({"msg": "Batch init for orders successful"})


# @app.get('/find/<order_id>')
# def find_order(order_id: str):
#     app.logger.info("entered find order")
#     print("entered find order")
#     order_entry: OrderValue = get_order_from_db(order_id)
#     return jsonify(
#         {
#             "order_id": order_id,
#             "paid": order_entry.paid,
#             "items": order_entry.items,
#             "user_id": order_entry.user_id,
#             "total_cost": order_entry.total_cost
#         }
#     )

# @app.get('/find_item/<item_id>')
# async def find_item(item_id: str):
#     app.logger.info("entered find item")
#     print("entered find item")
#     #save "entered" to a text file
#     try:
#         message = {'action': 'find', 'item_id': item_id}
#         async with producer.transaction():
#             await producer.send_and_wait('stock_request', value=msgpack.encode(message))
#             response = await consume()
#             if response['action'] == 'find':
#                 if response['status'] == '400':
#                     return abort(400, response['msg'])
#                 else:
#                     return jsonify(response['msg'])
#     except Exception as e:
#         return abort(400, f"Item: {item_id} not found!!!!!!!")
#     finally:
#         await stop_kafka_producer()



# def send_post_request(url: str):
#     try:
#         response = requests.post(url)
#     except requests.exceptions.RequestException:
#         abort(400, REQ_ERROR_STR)
#     else:
#         return response


# def send_get_request(url: str):
#     try:
#         response = requests.get(url)
#     except requests.exceptions.RequestException:
#         abort(400, REQ_ERROR_STR)
#     else:
#         return response


# @app.post('/addItem/<order_id>/<item_id>/<quantity>')
# def add_item(order_id: str, item_id: str, quantity: int):
#     order_entry: OrderValue = get_order_from_db(order_id)
#     item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
#     if item_reply.status_code != 200:
#         # Request failed because item does not exist
#         abort(400, f"Item: {item_id} does not exist!")
#     item_json: dict = item_reply.json()
#     order_entry.items.append((item_id, int(quantity)))
#     order_entry.total_cost += int(quantity) * item_json["price"]
#     try:
#         db.set(order_id, msgpack.encode(order_entry))
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
#                     status=200)


# def rollback_stock(removed_items: list[tuple[str, int]]):
#     for item_id, quantity in removed_items:
#         send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


# @app.post('/checkout/<order_id>')
# def checkout(order_id: str):
#     app.logger.debug(f"Checking out {order_id}")
#     order_entry: OrderValue = get_order_from_db(order_id)
#     order_lock = db.lock("order_lock")
#     # get the quantity per item
#     items_quantities: dict[str, int] = defaultdict(int)
#     for item_id, quantity in order_entry.items:
#         items_quantities[item_id] += quantity

#     if order_lock.acquire():
#         for item_id, quantity in items_quantities.items():
#             enough_stock = send_post_request(f"{GATEWAY_URL}/stock/check_stock/{item_id}/{quantity}")
#             if enough_stock.status_code != 200:
#                 # If one item does not have enough stock we need to rollback
#                 order_lock.release()
#                 abort(400, f'Out of stock on item_id: {item_id}')

#         enough_money = send_post_request(f"{GATEWAY_URL}/payment/check_money/{order_entry.user_id}/{order_entry.total_cost}")
#         if enough_money.status_code != 200:
#             # If the user does not have enough credit we need to rollback all the item stock subtractions
#             order_lock.release()
#             abort(400, "User out of credit")

#         for item_id, quantity in items_quantities.items():
#             stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
#             if stock_reply.status_code != 200:
#                 order_lock.release()
#                 abort(400, f'Out of stock on item_id: {item_id}')

#         user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
#         if user_reply.status_code != 200:
#             order_lock.release()
#             abort(400, "User out of credit")
#         order_entry.paid = True

#         try:
#             db.set(order_id, msgpack.encode(order_entry))
#         except redis.exceptions.RedisError:
#             order_lock.release()
#             return abort(400, DB_ERROR_STR)
#         order_lock.release()
#         app.logger.debug("Checkout successful")
#         return Response("Checkout successful", status=200)
    
# # with app.app_context():
# #     app.logger.info("Initializing")
# #     init_kafka_producer()


# # if __name__ == '__main__':
# #     app.run(host="0.0.0.0", port=8000, debug=True)
# #     # asyncio.Event().wait()  # Keep the service running

# async def main():
#     app.logger.info("Initializing 1")
#     with app.app_context():
#         app.logger.info("Initializing 2")
#         await init_kafka_producer()
#         app.run(host="0.0.0.0", port=8000, debug=True)

# if __name__ == '__main__':
#     app.logger.info("Initializing 3")
#     asyncio.run(main())
# else:
#     app.logger.info("Initializing 4")
#     gunicorn_logger = logging.getLogger('gunicorn.error')
#     app.logger.handlers = gunicorn_logger.handlers
#     app.logger.setLevel(gunicorn_logger.level)

import logging
import os
import atexit
import random
import uuid
from collections import defaultdict

import redis
import requests

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, AIOKafkaClient

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

KAFKA_SERVER = os.environ.get('KAFKA_SERVER', 'localhost:9092')

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

async def init_kafka_producer():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        enable_idempotence=True,
        transactional_id="order-service-transactional-id"
    )
    await producer.start()

async def stop_kafka_producer():
    await producer.stop()

async def consume():
    consumer = AIOKafkaConsumer(
        'stock_response',
        bootstrap_servers=KAFKA_SERVER,
        group_id="my-group")
    await consumer.start()
    try:
        async for msg in consumer:
            stock_data = msgpack.decode(msg.value)
            action = stock_data.action
            if action == 'find':
                item_id = stock_data.item_id
                item_entry = await find_item(item_id)
    finally:
        await consumer.stop()

def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        abort(400, f"Order: {order_id} not found!")
    return entry

@app.post('/create/<user_id>')
async def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})

@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
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
async def find_order(order_id: str):
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
    if producer is None:
        return abort(500, "Kafka producer is not initialized")

    try:
        message = {'action': 'find', 'item_id': item_id}
        async with producer.transaction():
            await producer.send_and_wait('stock_request', value=msgpack.encode(message))
            response = await consume()
            if response['action'] == 'find':
                if response['status'] == '400':
                    return abort(400, response['msg'])
                else:
                    return jsonify(response['msg'])
    except Exception as e:
        return abort(400, f"Item: {item_id} not found!!!!!!!")
    finally:
        await stop_kafka_producer()

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
async def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
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
async def checkout(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    order_lock = db.lock("order_lock")
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    if order_lock.acquire():
        for item_id, quantity in items_quantities.items():
            enough_stock = send_post_request(f"{GATEWAY_URL}/stock/check_stock/{item_id}/{quantity}")
            if enough_stock.status_code != 200:
                order_lock.release()
                abort(400, f'Out of stock on item_id: {item_id}')

        enough_money = send_post_request(f"{GATEWAY_URL}/payment/check_money/{order_entry.user_id}/{order_entry.total_cost}")
        if enough_money.status_code != 200:
            order_lock.release()
            abort(400, "User out of credit")

        for item_id, quantity in items_quantities.items():
            stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
            if stock_reply.status_code != 200:
                order_lock.release()
                abort(400, f'Out of stock on item_id: {item_id}')

        user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
        if user_reply.status_code != 200:
            order_lock.release()
            abort(400, "User out of credit")
        order_entry.paid = True

        try:
            db.set(order_id, msgpack.encode(order_entry))
        except redis.exceptions.RedisError:
            order_lock.release()
            return abort(400, DB_ERROR_STR)
        order_lock.release()
        return Response("Checkout successful", status=200)

async def main():
    await init_kafka_producer()
    app.run(host="0.0.0.0", port=8000)

if __name__ == '__main__':
    asyncio.run(main())

