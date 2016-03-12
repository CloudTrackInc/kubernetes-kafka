FROM wurstmeister/kafka

MAINTAINER CloudTrackInc

RUN java -version

ADD . /tmp/build
RUN cd /tmp/build && \
    ./gradlew -Dorg.gradle.native=false build && \
    cp build/libs/kubernetes-expander-1.0-SNAPSHOT $KAFKA_HOME/libs

ADD kafka-autoextend-partitions.sh /usr/bin/kafka-autoextend-partitions.sh
