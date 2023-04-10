import time
import sh
from retry import retry
from time import sleep
from sh import ssh
from txperf.redpanda_cluster import TimeoutException

import logging
logger = logging.getLogger("txperf")

class ZookeeperNode:
    def __init__(self, ip):
        self.ip = ip

class ZookeeperCluster:
    def __init__(self, nodes_path):
        self.nodes = []
        with open(nodes_path, "r") as f:
            for line in f:
                line = line.rstrip()
                parts = line.split(" ")
                self.nodes.append(ZookeeperNode(parts[0]))
    
    @retry(sh.ErrorReturnCode_255, tries=5, delay=0.2)
    def launch(self, node):
        ssh("ubuntu@" + node.ip, "/mnt/vectorized/control/zookeeper.start.sh")
    
    @retry(sh.ErrorReturnCode_255, tries=5, delay=0.2)
    def is_alive(self, node):
        result = ssh("ubuntu@" + node.ip, "/mnt/vectorized/control/zookeeper.alive.sh")
        return "YES" in result
    
    @retry(sh.ErrorReturnCode_255, tries=5, delay=0.2)
    def kill(self, node):
        ssh("ubuntu@" + node.ip, "/mnt/vectorized/control/zookeeper.stop.sh")

    @retry(sh.ErrorReturnCode_255, tries=5, delay=0.2)
    def clean(self, node):
        ssh("ubuntu@" + node.ip, "/mnt/vectorized/control/zookeeper.clean.sh")
    
    def kill_everywhere(self):
        for node in self.nodes:
            logger.debug(f"stopping a zookeeper instance on {node.ip}")
            self.kill(node)
    
    def wait_killed(self, timeout_s=10):
        begin = time.time()
        for node in self.nodes:
            while True:
                if time.time() - begin > timeout_s:
                    raise TimeoutException(f"zookeeper stuck and can't be stopped in {timeout_s} sec")
                logger.debug(f"checking if zookeeper process is running on {node.ip}")
                if not self.is_alive(node):
                    break
                sleep(1)
    
    def clean_everywhere(self):
        for node in self.nodes:
            logger.debug(f"cleaning a zookeeper instance on {node.ip}")
            self.clean(node)
    
    def launch_everywhere(self):
        for node in self.nodes:
            logger.debug(f"starting a zookeeper instance on {node.ip}")
            self.launch(node)

    def wait_alive(self, timeout_s=10):
        begin = time.time()
        for node in self.nodes:
            while True:
                if time.time() - begin > timeout_s:
                    raise TimeoutException(f"zookeeper process isn't running withing {timeout_s} sec")
                logger.debug(f"checking if zookeeper is running on {node.ip}")
                if self.is_alive(node):
                    break
                sleep(1)