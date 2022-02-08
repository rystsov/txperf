#!/bin/bash

set -e

if [ ! -f /mnt/vectorized/redpanda/pid ]; then
    echo "NO"
    exit 0
fi

pid=$(cat /mnt/vectorized/redpanda/pid)

if [ $pid == "" ]; then
    echo "NO"
    exit 0
fi

if process=$(ps -p $pid -o comm=); then
    echo "YES"
    exit 0
fi

echo "NO"
exit 0