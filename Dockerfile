FROM wurstmeister/kafka:0.10.2.1

MAINTAINER dawidCh

RUN java -version

ADD . /tmp/build
RUN cd /tmp/build && \
    ./gradlew -Dorg.gradle.native=false build && \
    cp build/libs/kubernetes-expander-2.0-SNAPSHOT.jar $KAFKA_HOME/libs/

ADD kafka-autoextend-partitions.sh /usr/bin/kafka-autoextend-partitions.sh
ADD kafka-kubernetes-start.sh /usr/bin/kafka-kubernetes-start.sh
RUN echo delete.topic.enable=true >> /opt/kafka/config/server.properties
RUN listeners=PLAINTEXTX://0.0.0.0:9092 >> /opt/kafka/config/server.properties

CMD ["kafka-kubernetes-start.sh"]
