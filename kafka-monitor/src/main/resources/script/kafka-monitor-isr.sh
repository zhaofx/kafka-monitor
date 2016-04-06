#!/bin/bash
cd `dirname $0`
source /etc/profile
java -cp kafka-monitor-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.letv.bigdata.kafka.monitor.KafkaTopicIsrMonitor
