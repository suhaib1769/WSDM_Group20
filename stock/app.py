import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, AIOKafkaClient
from quart import Quart, request, jsonify

DB_ERROR_STR = "DB error"
KAFKA_SERVER = os.environ.get('KAFKA_SERVER', 'kafka:9092')


app = Quart("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int

producer = None

async def init_kafka_producer():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        enable_idempotence=True,
        transactional_id="stock-service-transactional-id"
    )
    await producer.start()

async def stop_kafka_producer():
    await producer.stop()

async def consume():
    app.logger.info("Entered consume")
    consumer = AIOKafkaConsumer(
        'stock_request',
        bootstrap_servers='kafka:9092',
        group_id="my-group2",
        enable_auto_commit=False)
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        app.logger.info("Entered consume try")
        async for msg in consumer:
            app.logger.info("msg in consumer")
            stock_data = msgpack.decode(msg.value)
            action = stock_data['action']
            if action == 'find':
                app.logger.info("msg in action find consumer")
                item_id = stock_data["item_id"]
                app.logger.info("calling find item")
                item_entry = await find_item(item_id)

    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        # abort(400, f"Item: {item_id} not found!")
        return {'status': '400', 'msg': f"Item: {item_id} not found!"}

    item =  jsonify(
        {
            "stock": entry.stock,
            "price": entry.price
        }
    )
    return {'status': '200', 'msg': item} 


@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


# @app.get('/find/<item_id>')
# def find_item(item_id: str):
#     item_entry: StockValue = get_item_from_db(item_id)
#     return jsonify(
#         {
#             "stock": item_entry.stock,
#             "price": item_entry.price
#         }
#     )

async def find_item(item_id: str):
    app.logger.info("Entered stock find item")
    item_entry = get_item_from_db(item_id)

    try:
        message = {'action': 'find', 'msg': item_entry['msg'], "status": item_entry['status']}
        app.logger.info("Entered try find item")
        async with producer.transaction():
            app.logger.info("Before send and wait")
            await producer.send_and_wait('stock_response', value=msgpack.encode(message))
            app.logger.info("After send and wait")
    finally:
        await stop_kafka_producer()
    
   


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    # if item_entry.stock < 0:
    #     abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/check_stock/<item_id>/<amount>')
def check_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    return Response(f"Item: {item_id} has enough stock for order", status=200)

async def main():
    await init_kafka_producer()
    # await asyncio.Event().wait()  # Keep the service running
    await asyncio.gather(consume())


@app.before_serving
async def run_main():
    await main()

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
    try:
        asyncio.run(main()) 
    finally:
        asyncio.run(stop_kafka_producer())
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
