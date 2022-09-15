from txperf.workloads.tx_money import tx_money
from txperf.workloads.multi_write import multi_write
from txperf.workloads.tx_subscribe import tx_subscribe
from txperf.workloads.base_money import base_money

import logging

logger = logging.getLogger("txperf")

def tx_money_workload(nodes_path):
    writing_java = tx_money.Control()
    writing_java.launch = "/mnt/vectorized/control/tx-money.java.start.sh"
    writing_java.alive = "/mnt/vectorized/control/tx-money.java.alive.sh"
    writing_java.kill = "/mnt/vectorized/control/tx-money.java.stop.sh"
    writing_java.name = "tx-money / java"
    return tx_money.Workload(writing_java, nodes_path)

def multi_write_workload(nodes_path):
    writing_java = multi_write.Control()
    writing_java.launch = "/mnt/vectorized/control/multi-write.java.start.sh"
    writing_java.alive = "/mnt/vectorized/control/multi-write.java.alive.sh"
    writing_java.kill = "/mnt/vectorized/control/multi-write.java.stop.sh"
    writing_java.name = "multi-write / java"
    return multi_write.Workload(writing_java, nodes_path)

def base_money_workload(nodes_path):
    writing_java = base_money.Control()
    writing_java.launch = "/mnt/vectorized/control/base-money.java.start.sh"
    writing_java.alive = "/mnt/vectorized/control/base-money.java.alive.sh"
    writing_java.kill = "/mnt/vectorized/control/base-money.java.stop.sh"
    writing_java.name = "base-money / java"
    return base_money.Workload(writing_java, nodes_path)

def tx_subscribe_workload(nodes_path):
    writing_java = tx_subscribe.Control()
    writing_java.launch = "/mnt/vectorized/control/tx-subscribe.java.start.sh"
    writing_java.alive = "/mnt/vectorized/control/tx-subscribe.java.alive.sh"
    writing_java.kill = "/mnt/vectorized/control/tx-subscribe.java.stop.sh"
    writing_java.name = "tx-subscribe / java"
    return tx_subscribe.Workload(writing_java, nodes_path)

WORKLOADS = {
    "tx-money / java": tx_money_workload,
    "base-money / java": base_money_workload,
    "tx-subscribe / java": tx_subscribe_workload,
    "multi-write / java": multi_write_workload
}

def wait_all_workloads_killed(nodes_path):
    for key in WORKLOADS:
        logger.debug(f"stopping workload {key} everywhere (if running)")
        workload_cluster = WORKLOADS[key](nodes_path)
        workload_cluster.kill_everywhere()
        workload_cluster.wait_killed(timeout_s = 10)