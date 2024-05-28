#!/bin/sh

echo "Waiting for Kafka to be available..."

until nc -z kafka 9092; do
  sleep 5
done

echo "Kafka is up - executing command"
#exec uvicorn -b 0.0.0.0:5000 -w 2 --timeout 30 --log-level=info app:app --worker-class uvicorn.workers.UvicornWorker
exec $@