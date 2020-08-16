# Kafka Scripts

# Useful Commands
- Start Kafka `docker-compose up`
- Connect to container -> `docker exec -it atechguide-kafka bash`
- To execute commands go to `bin` directory -> `cd /opt/kafka_2.12-2.4.1/bin/`
- Create a Topic 
  - Without Partition -> `kafka-topics.sh --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic atechguide_first_topic`
  - With Partition -> `kafka-topics.sh --bootstrap-server localhost:9092 --create --partitions 3 --replication-factor 1 --topic atechguide_first_topic_partitioned`
- Connect to Kafka Consumer -> `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic atechguide_first_topic_partitioned`


# Reference 
- This project is build as a part of [Apache Kafka Series - Learn Apache Kafka for Beginners v2](https://www.udemy.com/course/apache-kafka/) Course
- This project also takes help of [rockthejvm.com  spark-streaming](https://rockthejvm.com/p/spark-streaming) Course
- [Wurstmeister/kafka-docker](https://github.com/wurstmeister/kafka-docker)
- [Jaceklaskowski apache-kafka/kafka-docker](https://jaceklaskowski.gitbooks.io/apache-kafka/kafka-docker.html)