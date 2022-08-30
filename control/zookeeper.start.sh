#!/bin/bash

set -e

if [ ! -f /mnt/vectorized/zookeeper/myid ]; then
    cp /mnt/vectorized/myid /mnt/vectorized/zookeeper/myid
fi

export ZOOKEEPER=/mnt/vectorized/bin/apache-zookeeper-3.8.0-bin

nohup $ZOOKEEPER/bin/zkServer.sh --config /mnt/vectorized start-foreground > /mnt/vectorized/zookeeper/log.zookeeper.$(date +%s) 2>&1 & echo $! > /mnt/vectorized/zookeeper/pid