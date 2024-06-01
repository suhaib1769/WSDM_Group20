import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from quart import Quart, request, jsonify

import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, AIOKafkaClient
from quart import Quart, request, jsonify, abort, Response


DB_ERROR_STR = "DB error"
KAFKA_SERVER = os.environ.get('KAFKA_SERVER', 'kafka:9092')


app = Quart("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()

producer = None
consumer = None

async def init_kafka_producer():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        enable_idempotence=True,
        transactional_id="payment-service-transactional-id"
    )
    await producer.start()

async def init_kafka_consumer():
    global consumer
    consumer = AIOKafkaConsumer(
        'payment_request',
        bootstrap_servers='kafka:9092',
        group_id="my-group2",
        enable_auto_commit=False,
        isolation_level="read_committed")
    # Get cluster layout and join group `my-group`
    await consumer.start()

async def stop_kafka_producer():
    await producer.stop()

async def consume():
    app.logger.info("Entered consume")
   
    try:
        # Consume messages
        app.logger.info("Entered consume try")
        async for msg in consumer:
            app.logger.info("msg in consumer")
            payment_data = msgpack.decode(msg.value)
            action = payment_data['action']
            if action == 'pay':
                app.logger.info("msg in action find consumer")
                user_id = payment_data['user_id']
                amount = payment_data['amount']
                await remove_credit(user_id, amount)



    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

atexit.register(close_db_connection)

#Introduce consumer/producer

class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
def create_user():
    print("Creating user")
    app.logger.info("Creating user")
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


# TODO: check later if a user can do more than one transaction at a time - in parallel
@app.post('/pay/<user_id>/<amount>')
async def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    message = {'action': 'pay', 'msg': f"User: {user_id} credit updated to: {user_entry.credit}", 'status': '200'}
    if user_entry.credit < 0:
        # abort(400, f"User: {user_id} credit cannot get reduced below zero!")
        # response = {'status': '400', 'msg': f"User: {user_id} credit cannot get reduced below zero!"}
        message = {'action': 'pay', 'msg': f"User: {user_id} credit cannot get reduced below zero!", 'status': '400'}
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        # return abort(400, DB_ERROR_STR)
        # response = {'status': '400', 'msg': DB_ERROR_STR}
        message = {'action': 'pay', 'msg': DB_ERROR_STR, 'status': '400'}
    
    try:
        app.logger.info("Entered try pay")
        async with producer.transaction():
            app.logger.info("Before send and wait")
            app.logger.info(message)
            await producer.send_and_wait('payment_response', value=msgpack.encode(message))
            app.logger.info("After send and wait")
    finally:
        await stop_kafka_producer()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
