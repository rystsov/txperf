#!/bin/bash

set -e

if [ ! -f /mnt/vectorized/zookeeper/pid ]; then
    exit 0
fi

pid=$(cat /mnt/vectorized/zookeeper/pid)

if [ $pid == "" ]; then
    exit 0
fi

if ps -p $pid; then
    kill -9 $pid
fi

rm /mnt/vectorized/zookeeper/pid