#!/bin/bash

set -e

nohup /mnt/vectorized/control/redpanda.loop.sh > /mnt/vectorized/redpanda/nohup.$(date +%s) 2>&1 & echo $! > /mnt/vectorized/redpanda/pid