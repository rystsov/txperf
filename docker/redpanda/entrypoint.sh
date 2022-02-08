#!/bin/bash

declare -A redpandas
declare -A node_ids

for (( i=1; i<=$REDPANDA_CLUSTER_SIZE; i++ )); do  
  redpandas["redpanda$i"]=""
  node_ids["redpanda$i"]="$i"
done

for host in "${!redpandas[@]}"; do
  redpandas[$host]=$(getent hosts $host | awk '{ print $1 }')
  while [ "${redpandas[$host]}" == "" ]; do
    sleep 1s
    redpandas[$host]=$(getent hosts $host | awk '{ print $1 }')
  done
done

mkdir -p /mnt/vectorized/redpanda/data
mkdir -p /mnt/vectorized/redpanda/coredump
mkdir -p /mnt/vectorized/kafka
mkdir -p /mnt/vectorized/zookeeper
mkdir -p /mnt/vectorized/monitoring

me=$(hostname)
myip="${redpandas[$me]}"

echo "tickTime=2000
initLimit=10
syncLimit=5
dataDir=/mnt/vectorized/zookeeper
clientPort=2181" > /mnt/vectorized/zoo.cfg
for (( i=1; i<=$REDPANDA_CLUSTER_SIZE; i++ )); do  
  host="redpanda$i"

  if [ "$me" == "$host" ]; then
    echo "$i" > /mnt/vectorized/myid
    # https://stackoverflow.com/questions/30940981/zookeeper-error-cannot-open-channel-to-x-at-election-address/30993130#30993130
    echo "server.$i=0.0.0.0:2888:3888" >> /mnt/vectorized/zoo.cfg
  else
    echo "server.$i=${redpandas[$host]}:2888:3888" >> /mnt/vectorized/zoo.cfg
  fi

  if [ "$me" == "$host" ]; then
  echo "broker.id=$i
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/mnt/vectorized/kafka
num.partitions=1
default.replication.factor=3
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=3
offsets.topic.num.partitions=1
offsets.commit.required.acks=-1
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
transaction.state.log.num.partitions=1
log.flush.interval.messages=1
log.flush.interval.ms=0
min.insync.replicas=2
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=${redpandas[$host]}:2181
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
listeners=PLAINTEXT://${redpandas[$host]}:9092
advertised.listeners=PLAINTEXT://${redpandas[$host]}:9092" >> /mnt/vectorized/kafka.properties
  fi
done

if [ "$me" == "redpanda1" ]; then
  rpk config bootstrap \
    --id ${node_ids[$me]} \
    --self $myip
else
  rpk config bootstrap \
    --id ${node_ids[$me]} \
    --self $myip \
    --ips "${redpandas[redpanda1]}"
fi

rpk config set redpanda.default_topic_partitions 1
rpk config set redpanda.default_topic_replications 3
rpk config set redpanda.transaction_coordinator_replication 3
rpk config set redpanda.id_allocator_replication 3
rpk config set redpanda.enable_leader_balancer false
rpk config set redpanda.enable_auto_rebalance_on_node_add false
rpk config set redpanda.enable_idempotence true
rpk config set redpanda.enable_transactions true
rpk config set redpanda.data_directory "/mnt/vectorized/redpanda/data"
rpk config set rpk.coredump_dir "/mnt/vectorized/redpanda/coredump"
rpk redpanda mode production
rpk redpanda tune all

rm -rf /mnt/vectorized/redpanda.nodes
for host in "${!redpandas[@]}"; do
  echo "${redpandas[$host]} ${node_ids[$host]}" >> /mnt/vectorized/redpanda.nodes
done
chown ubuntu:ubuntu -R /mnt/vectorized

service ssh start
sleep infinity