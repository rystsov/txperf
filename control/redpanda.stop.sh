#!/bin/bash

set -e

if [ ! -f /mnt/vectorized/redpanda/pid ]; then
    ps aux | grep [re]dpanda | grep -v redpanda.stop | awk '{print $2}' | xargs -r kill -9
    exit 0
fi

pid=$(cat /mnt/vectorized/redpanda/pid)

if [ $pid == "" ]; then
    ps aux | grep [re]dpanda | grep -v redpanda.stop | awk '{print $2}' | xargs -r kill -9
    exit 0
fi

if ps -p $pid; then
    kill -9 $pid
fi

ps aux | grep [re]dpanda | grep -v redpanda.stop | awk '{print $2}' | xargs -r kill -9

rm /mnt/vectorized/redpanda/pid