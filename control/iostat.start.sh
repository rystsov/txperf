#!/bin/bash

set -e

iostat | egrep Linux > /mnt/vectorized/monitoring/log.iostat
iostat | egrep Device >> /mnt/vectorized/monitoring/log.iostat
nohup bash -c 'iostat -d 1 | egrep -v "Device|loop|Linux|^$"' >> /mnt/vectorized/monitoring/log.iostat 2>&1 & echo $! > /mnt/vectorized/monitoring/pid.iostat