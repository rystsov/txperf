from sh import ssh
import sh
from retry import retry
import json
import requests
import os
import sys
import traceback
import time
from time import sleep
from txperf.redpanda_cluster import RedpandaNode, TimeoutException
from txperf.checks.result import Result
from txperf.workloads.base_money import stat

import logging
logger = logging.getLogger("txperf")

class Info:
    def __init__(self):
        self.succeeded_ops = 0
        self.failed_ops = 0
        self.timedout_ops = 0
        self.is_active = 0

class Control:
    def __init__(self):
        self.launch = None
        self.kill = None
        self.alive = None
        self.name = None

class Workload:
    def __init__(self, scripts, nodes_path):
        self.scripts = scripts
        self.nodes = []
        self.name = scripts.name
        with open(nodes_path, "r") as f:
            for line in f:
                line = line.rstrip()
                parts = line.split(" ")
                self.nodes.append(RedpandaNode(parts[0], int(parts[1])))

    @retry(sh.ErrorReturnCode_255, tries=5, delay=0.2)
    def heal(self):
        for node in self.nodes:
            ssh("ubuntu@" + node.ip, "/mnt/vectorized/control/network.heal.all.sh")
    
    @retry(sh.ErrorReturnCode_255, tries=5, delay=0.2)
    def is_alive(self, node):
        ip = node.ip
        result = ssh("ubuntu@"+ip, self.scripts.alive)
        return "YES" in result
    
    @retry(sh.ErrorReturnCode_255, tries=5, delay=0.2)
    def launch(self, node):
        ip = node.ip
        ssh("ubuntu@"+ip, self.scripts.launch)
    
    @retry(sh.ErrorReturnCode_255, tries=5, delay=0.2)
    def kill(self, node):
        ip = node.ip
        ssh("ubuntu@"+ip, self.scripts.kill)
    
    def kill_everywhere(self):
        for node in self.nodes:
            logger.debug(f"killing workload process on node {node.ip}")
            self.kill(node)
    
    def stop_everywhere(self):
        for node in self.nodes:
            logger.debug(f"stopping workload on node {node.ip}")
            self.stop(node)
    
    def wait_killed(self, timeout_s=10):
        begin = time.time()
        for node in self.nodes:
            while True:
                if time.time() - begin > timeout_s:
                    raise TimeoutException(f"workload stuck and can't be killed in {timeout_s} sec")
                logger.debug(f"checking if workload process is alive {node.ip}")
                if not self.is_alive(node):
                    break
                sleep(1)
    
    def launch_everywhere(self):
        for node in self.nodes:
            logger.debug(f"starting workload on node {node.ip}")
            self.launch(node)

    def wait_alive(self, timeout_s=10):
        begin = time.time()
        for node in self.nodes:
            while True:
                if time.time() - begin > timeout_s:
                    raise TimeoutException(f"workload process isn't running within {timeout_s} sec")
                logger.debug(f"checking if workload process is running on {node.ip}")
                if self.is_alive(node):
                    break
                sleep(1)
    
    def wait_ready(self, timeout_s=10):
        begin = time.time()
        for node in self.nodes:
            while True:
                if time.time() - begin > timeout_s:
                    raise TimeoutException(f"workload process isn't ready to accept requests within {timeout_s} sec")
                try:
                    logger.debug(f"checking if workload http api is ready on {node.ip}")
                    self.ping(node)
                    break
                except:
                    e, v = sys.exc_info()[:2]
                    trace = traceback.format_exc()
                    logger.error(e)
                    logger.error(v)
                    logger.error(trace)
                    if not self.is_alive(node):
                        logger.error(f"workload process on {node.ip} (id={node.id}) died")
                        raise
                sleep(1)
    
    def wait_progress(self, timeout_s=10):
        begin = time.time()
        started = dict()
        for node in self.nodes:
            started[node.ip]=self.info(node)
        made_progress = False
        progressed = dict()
        while True:
            made_progress = True
            for node in self.nodes:
                if time.time() - begin > timeout_s:
                    raise TimeoutException(f"workload haven't done progress within {timeout_s} sec")
                if node.ip in progressed:
                    continue
                logger.debug(f"checking if node {node.ip} made progress")
                info = self.info(node)
                if info.succeeded_ops > started[node.ip].succeeded_ops:
                    progressed[node.id]=True
                else:
                    made_progress = False
            if made_progress:
                break
            sleep(1)

    def emit_event(self, node, name, timeout_s=10):
        ip = node.ip
        r = requests.post(f"http://{ip}:8080/event/" + name, timeout=timeout_s)
        if r.status_code != 200:
            raise Exception(f"unexpected status code: {r.status_code}")

    def init(self, node, server, brokers, accounts, experiment, settings, timeout_s=10):
        ip = node.ip
        r = requests.post(f"http://{ip}:8080/init", json={
            "experiment": experiment,
            "server": server,
            "accounts": accounts,
            "brokers": brokers,
            "settings": settings}, timeout=timeout_s)
        if r.status_code != 200:
            logger.error(f"status code: {r.status_code}")
            logger.error(f"content: {r.content}")
            raise Exception(f"unexpected status code: {r.status_code}")
    
    def start(self, node, timeout_s=10):
        ip = node.ip
        r = requests.post(f"http://{ip}:8080/start", timeout=timeout_s)
        if r.status_code != 200:
            raise Exception(f"unexpected status code: {r.status_code}")

    def stop(self, node, timeout_s=10):
        ip = node.ip
        r = requests.post(f"http://{ip}:8080/stop", timeout=timeout_s)
        if r.status_code != 200:
            raise Exception(f"unexpected status code: {r.status_code}")

    def info(self, node):
        ip = node.ip
        r = requests.get(f"http://{ip}:8080/info")
        if r.status_code != 200:
            raise Exception(f"unexpected status code: {r.status_code}")
        info = Info()
        info.succeeded_ops = r.json()["succeeded_ops"]
        info.failed_ops = r.json()["failed_ops"]
        info.timedout_ops = r.json()["timedout_ops"]
        info.is_active = r.json()["is_active"]
        return info
    
    def ping(self, node):
        ip = node.ip
        r = requests.get(f"http://{ip}:8080/ping")
        if r.status_code != 200:
            logger.error(f"status code: {r.status_code}")
            logger.error(f"content: {r.content}")
            raise Exception(f"unexpected status code: {r.status_code}")
