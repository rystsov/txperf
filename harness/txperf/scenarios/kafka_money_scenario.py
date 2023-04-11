from sh import mkdir
from txperf.workloads.all import WORKLOADS, wait_all_workloads_killed
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

import logging

from txperf.redpanda_cluster import RedpandaCluster
from txperf.zookeeper_cluster import ZookeeperCluster
from txperf.kafka_cluster import KafkaCluster
from txperf.monitoring_cluster import MonitoringCluster

logger = logging.getLogger("txperf")

class KafkaMoneyScenario:
    SUPPORTED_WORKLOADS = {
        "tx-money / java", "base-money / java", "multi-write / java"
    }

    def __init__(self):
        self.redpanda_cluster = None
        self.zookeeper_cluster = None
        self.kafka_cluster = None
        self.workload_cluster = None
        self.config = None
        self.is_workload_log_fetched = False
        self.is_kafka_log_fetched = False
        self.is_monitoring_log_fetched = False

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
    
    def fetch_kafka_logs(self):
        if self.is_kafka_log_fetched:
            return
        if self.kafka_cluster != None:
            logger.info(f"stopping kafka")
            self.kafka_cluster.kill_everywhere()
            self.kafka_cluster.wait_killed(timeout_s=10)
            mkdir("-p", f"/mnt/vectorized/experiments/{self.config['experiment_id']}/kafka")
            for node in self.kafka_cluster.nodes:
                try:
                    mkdir("-p", f"/mnt/vectorized/experiments/{self.config['experiment_id']}/kafka/{node.ip}")
                    logger.info(f"fetching logs from {node.ip}")
                    scp(
                        f"ubuntu@{node.ip}:/mnt/vectorized/kafka/log.kafka.*",
                        f"/mnt/vectorized/experiments/{self.config['experiment_id']}/kafka/{node.ip}/")
                except:
                    pass
        if self.zookeeper_cluster != None:
            logger.info(f"stopping zookeeper")
            self.zookeeper_cluster.kill_everywhere()
            self.zookeeper_cluster.wait_killed(timeout_s=10)
            mkdir("-p", f"/mnt/vectorized/experiments/{self.config['experiment_id']}/kafka")
            for node in self.zookeeper_cluster.nodes:
                try:
                    mkdir("-p", f"/mnt/vectorized/experiments/{self.config['experiment_id']}/kafka/{node.ip}")
                    logger.info(f"fetching logs from {node.ip}")
                    scp(
                        f"ubuntu@{node.ip}:/mnt/vectorized/zookeeper/log.zookeeper.*",
                        f"/mnt/vectorized/experiments/{self.config['experiment_id']}/kafka/{node.ip}/")
                except:
                    pass
        self.is_kafka_log_fetched = True
    
    def fetch_monitoring_logs(self):
        if self.monitoring_cluster != None:
            if self.is_monitoring_log_fetched:
                return
            self.monitoring_cluster.kill_everywhere()
            self.monitoring_cluster.wait_killed(timeout_s=10)
            mkdir("-p", f"/mnt/vectorized/experiments/{self.config['experiment_id']}/monitoring")
            for node in self.monitoring_cluster.nodes:
                try:
                    logger.info(f"fetching monitoring data from {node.ip}")
                    mkdir("-p", f"/mnt/vectorized/experiments/{self.config['experiment_id']}/monitoring/{node.ip}")
                    scp(
                        f"ubuntu@{node.ip}:/mnt/vectorized/monitoring/log.iostat",
                        f"/mnt/vectorized/experiments/{self.config['experiment_id']}/monitoring/{node.ip}/")
                except:
                    pass
            self.is_monitoring_log_fetched = True
    
    def remove_logs(self):
        rm("-rf", f"/mnt/vectorized/experiments/{self.config['experiment_id']}/kafka")
    
    def measure_experiment(self):
        logger.info(f"start measuring")
        for node in self.workload_cluster.nodes:
            self.workload_cluster.emit_event(node, "measure")

        logger.info(f"wait for 180 seconds to record steady state")
        sleep(180)

        for node in self.workload_cluster.nodes:
            self.workload_cluster.emit_event(node, "measured")
    
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
            self.fetch_monitoring_logs()
        except:
            pass
        
        try:
            self.fetch_workload_logs()
        except:
            pass
        
        try:
            self.fetch_kafka_logs()
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
        accounts = self.config["accounts"]

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

        self.save_config()

        self.redpanda_cluster = RedpandaCluster("/mnt/vectorized/redpanda.nodes")
        logger.info(f"stopping redpanda cluster")
        self.redpanda_cluster.kill_everywhere()
        self.redpanda_cluster.wait_killed(timeout_s=10)

        logger.info(f"stopping monitoring")
        self.monitoring_cluster = MonitoringCluster("/mnt/vectorized/redpanda.nodes")
        self.monitoring_cluster.kill_everywhere()
        self.monitoring_cluster.wait_killed(timeout_s=10)
        self.monitoring_cluster.clean_everywhere()
        
        logger.info(f"(re-)starting fresh zookeeper & kafka cluster")
        self.kafka_cluster = KafkaCluster("/mnt/vectorized/redpanda.nodes")
        self.kafka_cluster.kill_everywhere()
        self.kafka_cluster.wait_killed(timeout_s=10)
        self.zookeeper_cluster = ZookeeperCluster("/mnt/vectorized/redpanda.nodes")
        self.zookeeper_cluster.kill_everywhere()
        self.zookeeper_cluster.wait_killed(timeout_s=10)
        
        self.zookeeper_cluster.clean_everywhere()
        self.zookeeper_cluster.launch_everywhere()
        self.zookeeper_cluster.wait_alive(timeout_s=20)
        self.kafka_cluster.clean_everywhere()
        self.kafka_cluster.launch_everywhere()
        self.kafka_cluster.wait_alive(timeout_s=20)

        logger.info(f"sleeping for 30s to let kafka start")
        sleep(30)

        for i in range(0, accounts):
            logger.info(f"creating \"acc{i}\" topic with replication factor 3")
            self.kafka_cluster.create_topic(f"acc{i}", 3, 1)

        logger.info(f"launching workload service")
        self.workload_cluster.launch_everywhere()
        self.workload_cluster.wait_alive(timeout_s=40)
        sleep(10)
        self.workload_cluster.wait_ready(timeout_s=80)

        for node in self.workload_cluster.nodes:
            logger.info(f"init workload with brokers=\"{self.redpanda_cluster.brokers()}\" and accounts=\"{accounts}\" on {node.ip}")
            self.workload_cluster.init(node, node.ip, self.redpanda_cluster.brokers(), accounts, self.config['experiment_id'], self.config["workload"]["settings"])

        for node in self.workload_cluster.nodes:
            logger.info(f"starting workload on {node.ip}")
            self.workload_cluster.start(node)
        
        ### distributing internal and data topic across different nodes
        
        logger.info(f"waiting for progress")
        self.workload_cluster.wait_progress(timeout_s=120)
        
        logger.info(f"warming up for 60s")
        sleep(60)

        logger.info(f"start monitoring")
        self.monitoring_cluster.launch_everywhere()