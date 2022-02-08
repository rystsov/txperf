#!/bin/bash

while true; do
/bin/redpanda --redpanda-cfg /etc/redpanda/redpanda.yaml > /mnt/vectorized/redpanda/log.$(date +%s) 2>&1 || true
done