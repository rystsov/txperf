from sh import mkdir
from txperf.workloads.all import WORKLOADS, wait_all_workloads_killed
from txperf.workloads.types import Selector
from time import sleep
from txperf.checks.result import Result
import copy
import os
from sh import scp, mkdir, rm
import json
import sh
import time
from time import sleep
import copy
import sys
import traceback
from txperf.redpanda_cluster import TimeoutException

import logging

from txperf.redpanda_cluster import RedpandaCluster
from txperf.zookeeper_cluster import ZookeeperCluster
from txperf.kafka_cluster import KafkaCluster

from confluent_kafka import (Producer, KafkaException)
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING

logger = logging.getLogger("txperf")

def init_group(brokers, group_id, topic, timeout_s):
    begin = time.time()
    while True:
        if time.time() - begin > timeout_s:
            raise TimeoutException(f"init transactions within {timeout_s} sec")
        try:
            c = Consumer({
                "bootstrap.servers": brokers,
                "enable.auto.commit": False,
                "group.id": group_id,
                "topic.metadata.refresh.interval.ms": 5000, # default: 300000
                "metadata.max.age.ms": 10000, # default: 900000
                "topic.metadata.refresh.fast.interval.ms": 250, # default: 250
                "topic.metadata.propagation.max.ms": 10000, # default: 30000
                "socket.timeout.ms": 10000, # default: 60000
                "connections.max.idle.ms": 0, # default: 0
                "reconnect.backoff.ms": 100, # default: 100
                "reconnect.backoff.max.ms": 10000, # default: 10000
                "statistics.interval.ms": 0, # default: 0
                "api.version.request.timeout.ms": 10000, # default: 10000
                "api.version.fallback.ms": 0, # default: 0
                "fetch.wait.max.ms": 500 # default: 0
            })
            c.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
            c.consume(timeout=10)
            return
        except:
            e, v = sys.exc_info()[:2]
            trace = traceback.format_exc()
            logger.error(e)
            logger.error(v)
            logger.error(trace)
            sleep(1)

def init_tx(brokers, tx_id, timeout_s):
    begin = time.time()
    while True:
        if time.time() - begin > timeout_s:
            raise TimeoutException(f"init transactions within {timeout_s} sec")
        try:
            producer = Producer({
                "bootstrap.servers": brokers,
                "enable.idempotence": True,
                "retries": 5,
                "transactional.id": tx_id
            })
            producer.init_transactions()
            return
        except:
            e, v = sys.exc_info()[:2]
            trace = traceback.format_exc()
            logger.error(e)
            logger.error(v)
            logger.error(trace)
            sleep(1)

class RedpandaSubscribeScenario:
    SUPPORTED_WORKLOADS = {
        "tx-subscribe / java"
    }

    def __init__(self):
        self.zookeeper_cluster = None
        self.kafka_cluster = None
        self.redpanda_cluster = None
        self.workload_cluster = None
        self.partitions = None
        self.config = None
        self.is_workload_log_fetched = False
        self.is_redpanda_log_fetched = False

    def validate(self, config):
        if config["workload"]["name"] not in self.SUPPORTED_WORKLOADS:
            raise Exception(f"unknown workload: {config['workload']}")
    
    def save_config(self):
        with open(f"/mnt/vectorized/experiments/{self.config['experiment_id']}/info.json", "w") as info:
            info.write(json.dumps(self.config, indent=2))
    
    def fetch_workload_logs(self):
        if self.workload_cluster != None:
            if self.is_workload_log_fetched:
                return
            logger.info(f"stopping workload everywhere")
            try:
                self.workload_cluster.stop_everywhere()
            except:
                pass
            self.workload_cluster.kill_everywhere()
            self.workload_cluster.wait_killed(timeout_s=10)
            for node in self.workload_cluster.nodes:
                try:
                    logger.info(f"fetching oplog from {node.ip}")
                    mkdir("-p", f"/mnt/vectorized/experiments/{self.config['experiment_id']}/{node.ip}")
                    scp(f"ubuntu@{node.ip}:/mnt/vectorized/workloads/logs/{self.config['experiment_id']}/{node.ip}/workload.log",
                        f"/mnt/vectorized/experiments/{self.config['experiment_id']}/{node.ip}/workload.log")
                    scp(f"ubuntu@{node.ip}:/mnt/vectorized/workloads/logs/system.log",
                        f"/mnt/vectorized/experiments/{self.config['experiment_id']}/{node.ip}/system.log")
                except:
                    pass
            self.is_workload_log_fetched = True
    
    def fetch_redpanda_logs(self):
        if self.redpanda_cluster != None:
            if self.is_redpanda_log_fetched:
                return
            logger.info(f"stopping redpanda")
            self.redpanda_cluster.kill_everywhere()
            self.redpanda_cluster.wait_killed(timeout_s=10)
            mkdir("-p", f"/mnt/vectorized/experiments/{self.config['experiment_id']}/redpanda")
            for node in self.redpanda_cluster.nodes:
                try:
                    mkdir("-p", f"/mnt/vectorized/experiments/{self.config['experiment_id']}/redpanda/{node.ip}")
                    logger.info(f"fetching logs from {node.ip}")
                    scp(
                        f"ubuntu@{node.ip}:/mnt/vectorized/redpanda/log.*",
                        f"/mnt/vectorized/experiments/{self.config['experiment_id']}/redpanda/{node.ip}/")
                except:
                    pass
            self.is_redpanda_log_fetched = True
    
    # TODO: add fetch monitoring
    
    def remove_logs(self):
        rm("-rf", f"/mnt/vectorized/experiments/{self.config['experiment_id']}/redpanda")
    
    def measure_experiment(self):
        logger.info(f"start measuring")
        for node in self.workload_cluster.nodes:
            self.workload_cluster.emit_event(node, "measure")

        logger.info(f"wait for 180 seconds to record steady state")
        sleep(180)

        for node in self.workload_cluster.nodes:
            self.workload_cluster.emit_event(node, "measured")

        self.fetch_workload_logs()
    
    def execute(self, config, experiment_id):
        try:
            self.prepare_experiment(config, experiment_id)
            self.measure_experiment()
        except:
            self.config["result"] = Result.more_severe(self.config["result"], Result.UNKNOWN)
            self.save_config()
            e, v = sys.exc_info()[:2]
            trace = traceback.format_exc()
            logger.error(v)
            logger.error(trace)
        
        try:
            self.fetch_workload_logs()
        except:
            pass
        
        try:
            self.fetch_redpanda_logs()
        except:
            pass

        if "settings" in self.config:
            if "remove_logs_on_success" in self.config["settings"]:
                if self.config["settings"]["remove_logs_on_success"]:
                    if self.config["result"] == Result.PASSED:
                        self.remove_logs()

        return self.config

    def prepare_experiment(self, config, experiment_id):
        self.config = copy.deepcopy(config)
        if "settings" not in self.config["workload"]:
            self.config["workload"]["settings"] = {}
        self.source = self.config["source"]
        self.target = self.config["target"]
        self.replication = self.config["replication"]
        self.partitions = self.config["partitions"]

        self.config["experiment_id"] = experiment_id
        self.config["result"] = Result.PASSED
        logger.info(f"starting experiment {self.config['name']} (id={self.config['experiment_id']})")
        
        mkdir("-p", f"/mnt/vectorized/experiments/{self.config['experiment_id']}")

        logger.info(f"stopping workload everywhere (if running)")
        wait_all_workloads_killed("/mnt/vectorized/client.nodes")

        self.workload_cluster = WORKLOADS[self.config["workload"]["name"]]("/mnt/vectorized/client.nodes")
        
        self.config["workload"]["nodes"] = []
        for node in self.workload_cluster.nodes:
            self.config["workload"]["nodes"].append(node.ip)

        logger.info(f"stopping kafka cluster")
        self.kafka_cluster = KafkaCluster("/mnt/vectorized/redpanda.nodes")
        self.kafka_cluster.kill_everywhere()
        self.kafka_cluster.wait_killed(timeout_s=10)

        logger.info(f"stopping zookeper cluster")
        self.zookeeper_cluster = ZookeeperCluster("/mnt/vectorized/redpanda.nodes")
        self.zookeeper_cluster.kill_everywhere()
        self.zookeeper_cluster.wait_killed(timeout_s=10)

        self.redpanda_cluster = RedpandaCluster("/mnt/vectorized/redpanda.nodes")

        self.config["brokers"] = self.redpanda_cluster.brokers()

        self.save_config()
        
        logger.info(f"(re-)starting fresh redpanda cluster")
        self.redpanda_cluster.kill_everywhere()
        self.redpanda_cluster.wait_killed(timeout_s=10)
        self.redpanda_cluster.clean_everywhere()
        self.redpanda_cluster.launch_everywhere()
        self.redpanda_cluster.wait_alive(timeout_s=10)

        # waiting for the controller to be up before creating a topic
        self.redpanda_cluster.wait_leader("controller", namespace="redpanda", replication=len(self.redpanda_cluster.nodes), timeout_s=30)

        logger.info(f"creating \"{self.source}\" topic with replication factor {self.replication} & {self.partitions} partitions")
        self.redpanda_cluster.create_topic(self.source, self.replication, self.partitions)
        logger.info(f"creating \"{self.target}\" topic with replication factor {self.replication} & {self.partitions} partitions")
        self.redpanda_cluster.create_topic(self.target, self.replication, self.partitions)
        self.redpanda_cluster.create_topic("init", 3, 1)
        self.redpanda_cluster.wait_leader("init", partition=0, replication=3, timeout_s=20)

        # waiting for the topic to come online
        for partition in range(0, self.partitions):
            self.redpanda_cluster.wait_leader(self.source, partition=partition, replication=self.replication, timeout_s=20)
            self.redpanda_cluster.wait_leader(self.target, partition=partition, replication=self.replication, timeout_s=20)

        init_tx(self.redpanda_cluster.brokers(), "tx-1", timeout_s=10)
        logger.info(f"waiting for id_allocator")
        self.redpanda_cluster.wait_leader("id_allocator", namespace="kafka_internal", replication=3, timeout_s=10)
        logger.info(f"waiting for tx coordinator")
        self.redpanda_cluster.wait_leader("tx", namespace="kafka_internal", replication=3, timeout_s=10)
        init_group(self.redpanda_cluster.brokers(), self.config["group_id"], "init", timeout_s=10)
        logger.info(f"waiting for group coordinator")
        self.redpanda_cluster.wait_leader("group", namespace="kafka_internal", replication=3, timeout_s=20)

        sleep(5)

        logger.info(f"launching workload service")
        self.workload_cluster.launch_everywhere()
        self.workload_cluster.wait_alive(timeout_s=10)
        sleep(10)
        self.workload_cluster.wait_ready(timeout_s=20)

        for node in self.workload_cluster.nodes:
            logger.info(f"init workload with brokers=\"{self.redpanda_cluster.brokers()}\", source=\"{self.source}\", target=\"{self.target}\" & group_ip=\"{self.config['group_id']}\" on {node.ip}")
            self.workload_cluster.init(node, node.ip, self.redpanda_cluster.brokers(), self.source, self.partitions, self.target, self.config['group_id'], self.config['experiment_id'], self.config["workload"]["settings"])

        for node in self.workload_cluster.nodes:
            logger.info(f"starting workload on {node.ip}")
            self.workload_cluster.start(node)
        
        for node in self.workload_cluster.nodes:
            self.workload_cluster.emit_event(node, "sync-1")
        for node in self.workload_cluster.nodes:
            self.workload_cluster.emit_event(node, "sync-2")
        
        ### distributing internal and data topic across different nodes
        
        logger.info(f"waiting for progress")
        self.workload_cluster.wait_progress(timeout_s=30, selector=Selector.ANY)

        for node in self.workload_cluster.nodes:
            self.workload_cluster.emit_event(node, "sync-3")
        for node in self.workload_cluster.nodes:
            self.workload_cluster.emit_event(node, "sync-4")
        
        logger.info(f"warming up for 60s")
        sleep(60)

        for node in self.workload_cluster.nodes:
            self.workload_cluster.emit_event(node, "sync-5")
        for node in self.workload_cluster.nodes:
            self.workload_cluster.emit_event(node, "sync-6")