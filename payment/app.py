import logging
import os
import atexit
import uuid
import threading

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

import pika
import json


DB_ERROR_STR = "DB error"


app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


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
    app.logger.info(f"Finding user: {user_id}")
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


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.info(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        # abort(400, f"User: {user_id} credit cannot get reduced below zero!")
        # return Response(f"User: {user_id} credit cannot get reduced below zero!", status=400)
        return {"status": 400, "message": f"User: {user_id} credit cannot get reduced below zero!"}
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        # return abort(400, DB_ERROR_STR)
        # return Response(DB_ERROR_STR, status=400)
        return {"status": 400, "message": DB_ERROR_STR}
    # return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)
    return {"status": 200, "message": f"User: {user_id} credit updated to: {user_entry.credit}"}

@app.post('/check_money/<user_id>/<amount>')
def check_money(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    return Response(f"User: {user_id} has enough credit", status=200)

def route_request(ch, method, properties, body):
    app.logger.info("Received item request")
    request = json.loads(body)
    if request["tag"] == "pay":
        app.logger.info("Paying")
        message = remove_credit(request["user_id"], request["amount"])
        app.logger.info(message)
        if message['status'] == 200:
            response = {"status": "success", "message":  "payment successfull"}
        else:
            response = {"status": "error", "message": "payment failed"}
    # if request["tag"] == "find_user":
    #     app.logger.info("finding user")
    #     user = get_user_from_db(request["user_id"])
    #     if user:
    #         app.logger.info("User found")
    #         response = {"status": "success", "user_id":  request["user_id"]}
    #     else:
    #         response = {"status": "error", "message": 'User not found'}
        
    else:
        response = {"status": "error", "message": 'NOT IT MY BOY'}
    
    app.logger.info(f"sending: " + request["tag"] + " " + response)
    channel.basic_publish(
            exchange="",
            routing_key="payment_request_response_queue",
            body=json.dumps(response),
        )
    app.logger.info(f"Processed request for: " + request["tag"])


def setup_rabbitmq():
    app.logger.info("Setting up RabbitMQ connection")
    global connection, channel
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq", blocked_connection_timeout=300))
        channel = connection.channel()
        # Declare queues
        channel.queue_declare(queue="payment_request_queue")
        channel.queue_declare(queue="payment_request_response_queue")
    except pika.exceptions.AMQPConnectionError as e:
        app.logger.info(f"Failed to connect to RabbitMQ: {e}")


def consume_messages():
    global connection, channel
    app.logger.info("Consuming messages from RabbitMQ")
    try:
        channel.basic_consume(
            queue="payment_request_queue",
            on_message_callback=route_request,
            auto_ack=True,
        )
        app.logger.info("Starting RabbitMQ payment consumer")
        channel.start_consuming()
    except Exception as e:
        app.logger.error(f"Error in RabbitMQ payment consumer: {e}")
        if connection and connection.is_open:
            connection.close()


if __name__ == "__main__":
    # Setup RabbitMQ connection and channel
    producer_thread = threading.Thread(target=setup_rabbitmq)
    producer_thread.start()

    # Start RabbitMQ consumer in a separate thread
    consumer_thread = threading.Thread(target=consume_messages)
    consumer_thread.start()

    # Start Flask app in the main thread
    app.run(host="0.0.0.0", port=8000, debug=True)
    app.logger.info("Starting payment service1")

else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    # Setup RabbitMQ connection and channel
    setup_rabbitmq()
    # Start RabbitMQ consumer in a separate thread
    consumer_thread = threading.Thread(target=consume_messages)
    consumer_thread.start()
    app.logger.info("Starting payment service2")


# if __name__ == '__main__':
#     app.run(host="0.0.0.0", port=8000, debug=True)
# else:
#     gunicorn_logger = logging.getLogger('gunicorn.error')
#     app.logger.handlers = gunicorn_logger.handlers
#     app.logger.setLevel(gunicorn_logger.level)
