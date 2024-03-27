# MangolaStreaming
A streaming demo is about Mangola Platform using Kafka and Flink

# Prepare for running.

1. Building packages.
```
mvn clean package
```

2. Start Kafka server. (
```
~/kafka_2.13-3.6.0/bin/kafka-server-start.sh ~/kafka_2.13-3.6.0/config/kraft/server.properties
```

3. Start and running flink server.

```
../flink-1.18.1/bin/start-cluster.sh
```

```
../flink-1.18.1/bin/flink run -p 4 ./target/magolastreaming-1.0-SNAPSHOT.jar
```

![image](https://github.com/quangtn266/MangolaStreaming/assets/50879191/462cb720-9d51-43ba-9abe-2bcabda0c6fb)
