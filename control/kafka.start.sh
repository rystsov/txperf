#!/bin/bash

set -e

export KAFKA_HEAP_OPTS="-Xms16g -Xmx16g -XX:MetaspaceSize=96m"
export KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UnlockExperimentalVMOptions -XX:+UseZGC -XX:+ParallelRefProcEnabled -XX:+AggressiveOpts -XX:+DoEscapeAnalysis -XX:ParallelGCThreads=12 -XX:ConcGCThreads=12 -XX:+DisableExplicitGC -XX:-ResizePLAB -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 -Djava.awt.headless=true"
export KAFKA=/mnt/vectorized/bin/kafka_2.13-3.0.0
nohup $KAFKA/bin/kafka-server-start.sh /mnt/vectorized/kafka.properties > /mnt/vectorized/kafka/log.kafka.$(date +%s) 2>&1 & echo $! > /mnt/vectorized/kafka/pid