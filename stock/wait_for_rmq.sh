#!/bin/sh

echo "Waiting for RabbitMQ to be available..."

until nc -z rabbitmq-service 5672; do
  sleep 5
done

echo "RabbitMQ is up - executing command"
exec "$@"
