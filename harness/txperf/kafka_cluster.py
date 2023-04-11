import time
from time import sleep
from sh import ssh
import sh
from retry import retry
from txperf.redpanda_cluster import TimeoutException

import logging
logger = logging.getLogger("txperf")

class KafkaNode:
    def __init__(self, ip):
        self.ip = ip

class KafkaCluster:
    def __init__(self, nodes_path):
        self.nodes = []
        with open(nodes_path, "r") as f:
            for line in f:
                line = line.rstrip()
                parts = line.split(" ")
                self.nodes.append(KafkaNode(parts[0]))
    
    @retry(sh.ErrorReturnCode_255, tries=5, delay=0.2)
    def launch(self, node):
        ssh("ubuntu@" + node.ip, "/mnt/vectorized/control/kafka.start.sh")
    
    @retry(sh.ErrorReturnCode_255, tries=5, delay=0.2)
    def is_alive(self, node):
        result = ssh("ubuntu@" + node.ip, "/mnt/vectorized/control/kafka.alive.sh")
        return "YES" in result
    
    @retry(sh.ErrorReturnCode_255, tries=5, delay=0.2)
    def kill(self, node):
        ssh("ubuntu@" + node.ip, "/mnt/vectorized/control/kafka.stop.sh")

    @retry(sh.ErrorReturnCode_255, tries=5, delay=0.2)
    def clean(self, node):
        ssh("ubuntu@" + node.ip, "/mnt/vectorized/control/kafka.clean.sh")
    
    def kill_everywhere(self):
        for node in self.nodes:
            logger.debug(f"stopping a kafka instance on {node.ip}")
            self.kill(node)
    
    def wait_killed(self, timeout_s=10):
        begin = time.time()
        for node in self.nodes:
            while True:
                if time.time() - begin > timeout_s:
                    raise TimeoutException(f"kafka stuck and can't be stopped in {timeout_s} sec")
                logger.debug(f"checking if kafka process is running on {node.ip}")
                if not self.is_alive(node):
                    break
                sleep(1)
    
    def clean_everywhere(self):
        for node in self.nodes:
            logger.debug(f"cleaning a kafka instance on {node.ip}")
            self.clean(node)
    
    def launch_everywhere(self):
        for node in self.nodes:
            logger.debug(f"starting a kafka instance on {node.ip}")
            self.launch(node)

    def wait_alive(self, timeout_s=10):
        begin = time.time()
        for node in self.nodes:
            while True:
                if time.time() - begin > timeout_s:
                    raise TimeoutException(f"kafka process isn't running withing {timeout_s} sec")
                logger.debug(f"checking if kafka is running on {node.ip}")
                if self.is_alive(node):
                    break
                sleep(1)
    
    def brokers(self):
        return ",".join(map(lambda x: x.ip+":9092", self.nodes))
    
    @retry(sh.ErrorReturnCode_255, tries=5, delay=0.2)
    def create_topic(self, topic, replication, partitions):
        ssh("ubuntu@" + self.nodes[0].ip, "rpk", "topic", "create", "--brokers", self.brokers(), topic, "-r", replication, "-p", partitions)