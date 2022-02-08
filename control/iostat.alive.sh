#!/bin/bash

set -e

if [ ! -f /mnt/vectorized/monitoring/pid.iostat ]; then
    echo "NO"
    exit 0
fi

pid=$(cat /mnt/vectorized/monitoring/pid.iostat)

if [ $pid == "" ]; then
    echo "NO"
    exit 0
fi

if process=$(ps -p $pid -o comm=); then
    if [ $process == "bash" ]; then
        echo "YES"
        exit 0
    fi
fi

echo "NO"
exit 0