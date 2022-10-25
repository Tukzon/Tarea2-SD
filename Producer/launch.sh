#!/bin/bash

set -m
echo "Launching Producer"
python ./app.py &
fg %1
echo "Creating topics"
docker exec -it kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic ventas
docker exec -it kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic stock
docker exec -it kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic coordenadas


