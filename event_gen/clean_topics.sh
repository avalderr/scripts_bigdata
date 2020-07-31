#!/bin/bash
list=$(cat topic_list.txt)
for topic in $list ; do
	echo "limpiando topic $topic"
	/opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --topic $topic --delete
	/opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $topic
	echo "topic $topic OK"
done
