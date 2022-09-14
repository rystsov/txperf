#!/bin/bash

set -e

cd /mnt/vectorized/workloads/logs
nohup java -Xms16G -Xmx16G -XX:+UseG1GC -XX:MaxGCPauseMillis=10 -XX:+ParallelRefProcEnabled -XX:+UnlockExperimentalVMOptions -XX:+AggressiveOpts -XX:+DoEscapeAnalysis -XX:ParallelGCThreads=32 -XX:ConcGCThreads=32 -XX:G1NewSizePercent=50 -XX:+DisableExplicitGC -XX:-ResizePLAB -XX:+PerfDisableSharedMem -XX:+AlwaysPreTouch -XX:-UseBiasedLocking -cp /mnt/vectorized/workloads/target/workloads-1.0-SNAPSHOT.jar:/mnt/vectorized/workloads/target/dependency/* io.vectorized.base_money.App > /mnt/vectorized/workloads/logs/system.log 2>&1 & echo $! > /mnt/vectorized/workloads/logs/base-money.pid