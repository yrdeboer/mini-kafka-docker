### Introduction
This is a mini-project to refresh my memory about Docker and Kafka.

Following the steps below a Kafka cluster is set up.
A topic is added.

Then a MySQL server Docker image is started and also a Kafka custom consumer,
written in Python, is started inside a Docker image.

A simple producer then streams data to the Kafka cluster. It mimicks intermittendly
received bitcoin price data.

The consumer will receive it from the Kafka cluster and insert it into
the MySQL database.


### Start local zookeeper in background
./kafka_2.12-2.6.3/bin/zookeeper-server-start.sh ./kafka_2.12-2.6.3/config/zookeeper.properties   &> out.zookeeper.txt &

### Start first broker at 9090 on local host in background
./kafka_2.12-2.6.3/bin/kafka-server-start.sh kafka_2.12-2.6.3/config/server-100.properties   &> out.broker.txt &

### Create the price data topic
./kafka_2.12-2.6.3/bin/kafka-topics.sh --create --topic MyPriceData --zookeeper localhost:2181 --partitions 3 --replication-factor 1

### Inspect
./kafka_2.12-2.6.3/bin/kafka-topics.sh --describe  --topic MyPriceData --bootstrap-server localhost:9090

### Create consumer container
docker build -t consumer -f Dockerfile-consumer .

### Start mysql server with fresh database named items
docker run -d --name mysql-container -e MYSQL_ROOT_PASSWORD=r00tpa55 -e MYSQL_DATABASE=items -e MYSQL_USER=user1 -e MYSQL_PASSWORD=mypa55  -p 30306:3306 mysql

### Start consumer container linked with mysql container
docker run -d --name consumer-container -e HOST_IP=\`hostname -I | awk '{ print $1 }'\` --link mysql-container:mysql  consumer

### Follow consumer logs to see when connection is up
docker logs -f consumer-container

### Start locally producer in background
python3 producer.py &> out.producer.txt &

### Remove topic
./kafka_2.12-2.6.3/bin/kafka-topics.sh --delete --topic MyPriceData --zookeeper localhost:2181

### Cleanup, kafka stop script does not work, brute force kill brokers and zookeeper
ps aux | grep java | awk '{ print $2 }' | xargs kill -9

### Remove temp log dir of broker
rm -fr /tmp/kafka-logs-100 /tmp/zookeeper/

### Cleanup local dir
rm out*txt

### Stop docker images
docker stop consumer-container mysql-container

### Remove them too
docker rm consumer-container mysql-container
