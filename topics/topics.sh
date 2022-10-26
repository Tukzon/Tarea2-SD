#!/bin/bash
echo "Runing server"
sleep `10`
echo "Creating topics"
kafka-topics.sh --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 2 --topic ventas
kafka-topics.sh --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 2 --topic stock
kafka-topics.sh --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 2 --topic coordenadas


