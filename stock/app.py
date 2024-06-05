import logging
import os
import atexit
import time
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"

app = Flask("stock-service")

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
        abort(400, f"Item: {item_id} not found!")
    return entry


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


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


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


@app.post('/subtract/<item_id>/<amount>/<order_transaction_id>')
def remove_stock(item_id: str, amount: int, order_transaction_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    app.logger.info(f"Stock service : entered subtract")
    if db.hmget(order_transaction_id, f'subtract_commit_{item_id}')[0] == str(1).encode():
        return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)
    transaction_id_subtract_stock = str(uuid.uuid4())
    db.hmset(order_transaction_id, {transaction_id_subtract_stock: msgpack.encode(f'StockService: Started {transaction_id_subtract_stock}: Subtract stock for item: {item_id}, Amount: {amount}')})
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    try:    
        db.hmset(order_transaction_id, {transaction_id_subtract_stock: msgpack.encode(f'StockService: Commit {transaction_id_subtract_stock}: Subtract stock for item: {item_id}, Amount: {amount}'), f'subtract_commit_{item_id}': 1})
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:    
        db.hmset(order_transaction_id, {transaction_id_subtract_stock: msgpack.encode(f'StockService: Failed {transaction_id_subtract_stock}: Subtract stock for item: {item_id}, Amount: {amount}')})
        return abort(400, DB_ERROR_STR)
    db.hmset(order_transaction_id, {transaction_id_subtract_stock: msgpack.encode(f'StockService: Success {transaction_id_subtract_stock}: Subtract stock for item: {item_id}, Amount: {amount}')})
    app.logger.info("Stock service : adding sleep before response")
    time.sleep(20)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/check_stock/<item_id>/<amount>/<order_transaction_id>')
def check_stock(item_id: str, amount: int, order_transaction_id: str):
    item_entry: StockValue = get_item_from_db(item_id)   
    transaction_id_check_stock = str(uuid.uuid4())
    db.hmset(order_transaction_id, {transaction_id_check_stock: msgpack.encode(f'StockService: Started {transaction_id_check_stock}: Check stock for item: {item_id}, Amount: {amount}')})
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    if item_entry.stock < 0:    
        db.hmset(order_transaction_id, {transaction_id_check_stock: msgpack.encode(f'StockService: Failed {transaction_id_check_stock}: Check stock for item: {item_id}, Amount: {amount}')})
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    db.hmset(order_transaction_id, {transaction_id_check_stock: msgpack.encode(f'StockService: Success {transaction_id_check_stock}: Check stock for item: {item_id}, Amount: {amount}')})
    return Response(f"Item: {item_id} has enough stock for order", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
