#!/bin/bash
list=$(cat topic_list.txt)
for topic in $list ; do
	echo "creando topic $topic"
	/opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $topic
done
