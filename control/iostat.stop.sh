#!/bin/bash

set -e

if [ ! -f /mnt/vectorized/monitoring/pid.iostat ]; then
    exit 0
fi

pid=$(cat /mnt/vectorized/monitoring/pid.iostat)

if [ $pid == "" ]; then
    exit 0
fi

if ps -p $pid; then
    kill -9 $pid
fi

rm /mnt/vectorized/monitoring/pid.iostat