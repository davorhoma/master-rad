#!/bin/bash

BOOTSTRAP_SERVER="broker1:9092"

echo "Kreiranje Kafka topic-a za YouTube i TikTok (sa DLQ)..."

# 1. Ulazni topic-i
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
	--topic yt-monitoring-topic --replication-factor 1 --partitions 4

/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
	--topic tt-monitoring-topic --replication-factor 1 --partitions 4

/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
	--topic yt-search-topic --replication-factor 1 --partitions 4

/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
	--topic tt-search-topic --replication-factor 1 --partitions 4

# 2. Topic za neuspešne poruke (Dead Letter Queue)
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --create --if-not-exists \
	--topic social-media-processing-failed \
	--replication-factor 1 --partitions 2

echo "Svi topic-i, uključujući i failed/DLQ, uspešno su kreirani!"
