from sh import gnuplot, rm
import jinja2
import sys
import traceback
import json
import os
from txperf.checks.result import Result
from txperf.workloads.tx_money.log_utils import State, cmds, threads
import logging

logger = logging.getLogger("stat")

LATENCY = """
set terminal png size 1600,1200
set output "percentiles.png"
set title "{{ title }}"
set multiplot
set yrange [0:{{ yrange }}]
set xrange [-0.1:1.1]

plot "percentiles.log" using 1:2 title "latency (us)" with line lt rgb "black",\\
     {{p99}} title "p99" with lines lt 1

unset multiplot
"""

AVAILABILITY = """
set terminal png size 1600,1200
set output "availability.png"
set title "{{ title }}"
show title
plot "availability.log" using ($1/1000):2 title "unavailability (us)" w p ls 7
"""

OVERVIEW = """
set terminal png size 1600,1200
set output "overview.png"
set multiplot
set lmargin 10
set rmargin 10

set pointsize 0.2
set xrange [0:{{ duration }}]

set yrange [0:{{ commit_boundary }}]
set size 1, 0.15
set origin 0, 0

set title "commit time"
show title

plot 'latency_commit.log' using ($1/1000):2 notitle with points lt rgb "black" pt 7,\\
     {{commit_p99}} title "p99" with lines lt 1

set yrange [0:{{ send_boundary }}]
set size 1, 0.15
set origin 0, 0.15

set title "send time"
show title

plot 'latency_send.log' using ($1/1000):2 notitle with points lt rgb "black" pt 7,\\
     {{send_p99}} title "p99" with lines lt 1

set notitle

set y2range [0:{{ big_latency }}]
set yrange [0:{{ big_latency }}]
set size 1, 0.35
set origin 0, 0.3
unset ytics
set y2tics auto
set tmargin 0
set border 11

plot 'latency_ok.log' using ($1/1000):2 title "latency ok (us)" with points lt rgb "black" pt 7,\\
     'latency_err.log' using ($1/1000):2 title "latency err (us)" with points lt rgb "red" pt 7,\\
     'latency_timeout.log' using ($1/1000):2 title "latency timeout (us)" with points lt rgb "blue" pt 7,\\
     {{p99}} title "p99" with lines lt 1

set title "{{ title }}"
show title

set yrange [0:{{ throughput }}]

set size 1, 0.35
set origin 0, 0.65
set format x ""
set bmargin 0
set tmargin 3
set border 15
unset y2tics
set ytics

plot 'throughput.log' using ($1/1000):2 title "throughput (1s)" with line lt rgb "black"

unset multiplot
"""

class Throughput:
    def __init__(self):
        self.count = 0
        self.time_ms = 0
        self.history = []
    
    def tick(self, now_ms):
        while self.time_ms + 1000 < now_ms:
            ts = int(self.time_ms+1000)
            self.history.append([ts, self.count])
            self.count = 0
            self.time_ms += 1000

class ThroughputBuilder:
    def __init__(self):
        self.total_throughput = None
    
    def build(self, config, latencies):
        self.total_throughput = Throughput()
        
        for [ts_ms, _] in latencies:
            self.total_throughput.tick(ts_ms)
            self.total_throughput.count += 1

class LogPlayer:
    def __init__(self, config):
        self.config = config
        
        self.curr_state = dict()
        self.thread_type = dict()

        self.started_us = None
        
        self.ts_us = None

        self.latency_err_history = []
        self.latency_ok_history = []
        self.latency_commit_history = []
        self.latency_send_history = []

        self.txn_started = dict()
        self.end_txn_started = dict()
        self.is_commit = dict()

        self.should_measure = False
        self.stop_measure = False
    
    def transferring_apply(self, thread_id, parts):
        if self.curr_state[thread_id] == State.TX:
            self.txn_started[thread_id] = self.ts_us
        elif self.curr_state[thread_id] == State.COMMIT:
            if self.should_measure:
                self.latency_send_history.append([int((self.ts_us-self.started_us)/1000), self.ts_us-self.txn_started[thread_id]])
            self.is_commit[thread_id] = True
            self.end_txn_started[thread_id] = self.ts_us
        elif self.curr_state[thread_id] == State.ABORT:
            self.is_commit[thread_id] = False
            self.end_txn_started[thread_id] = self.ts_us
        elif self.curr_state[thread_id] == State.OK:
            if self.should_measure:
                if self.is_commit[thread_id]:
                    self.latency_ok_history.append([int((self.ts_us-self.started_us)/1000), self.ts_us-self.txn_started[thread_id]])
                    self.latency_commit_history.append([int((self.ts_us-self.started_us)/1000), self.ts_us-self.end_txn_started[thread_id]])
                else:
                    self.latency_err_history.append([int((self.ts_us-self.started_us)/1000), self.ts_us-self.txn_started[thread_id]])
    
    def apply(self, line):
        if self.stop_measure:
            return
        
        parts = line.rstrip().split('\t')

        if parts[2] not in cmds:
            raise Exception(f"unknown cmd \"{parts[2]}\"")

        if self.ts_us == None:
            self.ts_us = int(parts[1])
            self.started_us = self.ts_us
        else:
            delta_us = int(parts[1])
            self.ts_us = self.ts_us + delta_us
        
        new_state = cmds[parts[2]]

        if new_state == State.EVENT:
            name = parts[3]
            if name == "measure" and not self.should_measure:
                self.started_us = self.ts_us
                self.should_measure = True
            if name == "measured":
                self.stop_measure = True
                return
            return
        
        thread_id = int(parts[0])
        if thread_id not in self.curr_state:
            self.thread_type[thread_id] = parts[4]
            self.curr_state[thread_id] = None
            if self.thread_type[thread_id] not in threads:
                raise Exception(f"unknown thread type: {parts[4]}")
        if self.curr_state[thread_id] == None:
            if new_state != State.STARTED:
                raise Exception(f"first logged command of a new thread should be started, got: \"{parts[2]}\"")
            self.curr_state[thread_id] = new_state
        else:
            if new_state not in threads[self.thread_type[thread_id]][self.curr_state[thread_id]]:
                raise Exception(f"unknown transition {self.curr_state[thread_id]} -> {new_state}")
            self.curr_state[thread_id] = new_state

        if self.thread_type[thread_id] == "transferring":
            self.transferring_apply(thread_id, parts)

class StatInfo:
    def __init__(self):
        self.latency_err_history = []
        self.latency_ok_history = []
        self.latency_commit_history = []
        self.latency_send_history = []

def render_overview(config, workload_dir, stat):
    latency_commit_log_path = os.path.join(workload_dir, "latency_commit.log")
    latency_send_log_path = os.path.join(workload_dir, "latency_send.log")
    latency_ok_log_path = os.path.join(workload_dir, "latency_ok.log")
    latency_err_log_path = os.path.join(workload_dir, "latency_err.log")
    
    throughput_log_path = os.path.join(workload_dir, "throughput.log")
    overview_gnuplot_path = os.path.join(workload_dir, "overview.gnuplot")

    try:
        latency_ok = open(latency_ok_log_path, "w")
        latency_err = open(latency_err_log_path, "w")
        latency_commit = open(latency_commit_log_path, "w")
        latency_send = open(latency_send_log_path, "w")
        throughput_log = open(throughput_log_path, "w")
        
        duration_ms = 0
        min_latency_us = None
        max_latency_us = 0
        commit_p99 = None
        commit_min = None
        commit_max = None
        send_p99 = None
        send_min = None
        send_max = None
        max_unavailability_us = 0
        max_throughput = 0

        latencies = []
        for [_, latency_us] in stat.latency_ok_history:
            latencies.append(latency_us)
        latencies.sort()
        p99  = latencies[int(0.99*len(latencies))]
        p50  = latencies[int(0.5*len(latencies))]

        latencies = []
        for [_, latency_us] in stat.latency_commit_history:
            latencies.append(latency_us)
        latencies.sort()
        commit_p99 = latencies[int(0.99*len(latencies))]
        commit_p50 = latencies[int(0.5*len(latencies))]
        commit_min = latencies[0]
        commit_max = latencies[-1]

        latencies = []
        for [_, latency_us] in stat.latency_send_history:
            latencies.append(latency_us)
        latencies.sort()
        send_p99 = latencies[int(0.99*len(latencies))]
        send_p50 = latencies[int(0.5*len(latencies))]
        send_min = latencies[0]
        send_max = latencies[-1]

        for [ts_ms,latency_us] in stat.latency_send_history:
            duration_ms = max(duration_ms, ts_ms)
            latency_send.write(f"{ts_ms}\t{latency_us}\n")
        
        for [ts_ms,latency_us] in stat.latency_commit_history:
            duration_ms = max(duration_ms, ts_ms)
            latency_commit.write(f"{ts_ms}\t{latency_us}\n")

        max_unavailability_ms = 0
        last_ok = stat.latency_ok_history[0][0]
        for [ts_ms,latency_us] in stat.latency_ok_history:
            max_unavailability_ms = max(max_unavailability_ms, ts_ms - last_ok)
            last_ok = ts_ms
            duration_ms = max(duration_ms, ts_ms)
            if min_latency_us == None:
                min_latency_us = latency_us
            min_latency_us = min(min_latency_us, latency_us)
            max_latency_us = max(max_latency_us, latency_us)
            latency_ok.write(f"{ts_ms}\t{latency_us}\n")
        max_unavailability_us = 1000 * max_unavailability_ms
        
        for [ts_ms,latency_us] in stat.latency_err_history:
            duration_ms = max(duration_ms, ts_ms)
            max_latency_us = max(max_latency_us, latency_us)
            latency_err.write(f"{ts_ms}\t{latency_us}\n")
        
        throughput_builder = ThroughputBuilder()
        throughput_builder.build(config, stat.latency_ok_history)
        
        for [ts_ms, count] in throughput_builder.total_throughput.history:
            duration_ms = max(duration_ms, ts_ms)
            max_throughput = max(max_throughput, count)
            throughput_log.write(f"{ts_ms}\t{count}\n")
        
        latency_ok.close()
        latency_err.close()
        latency_commit.close()
        latency_send.close()
        throughput_log.close()
        
        with open(overview_gnuplot_path, "w") as gnuplot_file:
            gnuplot_file.write(
                jinja2.Template(OVERVIEW).render(
                    title = config["name"],
                    duration=int(duration_ms/1000),
                    big_latency=int(p99*1.2),
                    p99=p99,
                    commit_p99=commit_p99,
                    commit_boundary=int(commit_p99*1.2),
                    send_p99=send_p99,
                    send_boundary=int(send_p99*1.2),
                    throughput=int(max_throughput*1.2)))
        
        gnuplot(overview_gnuplot_path, _cwd=workload_dir)
        ops = len(stat.latency_ok_history)

        return {
            "result": Result.PASSED,
            "latency_us": {
                "tx": {
                    "min": min_latency_us,
                    "max": max_latency_us,
                    "p99": p99,
                    "p50": p50
                },
                "commit": {
                    "min": commit_min,
                    "max": commit_max,
                    "p99": commit_p99,
                    "p50": commit_p50
                },
                "send": {
                    "min": send_min,
                    "max": send_max,
                    "p99": send_p99,
                    "p50": send_p50,
                }
            },
            "max_unavailability_us": max_unavailability_us,
            "throughput": {
                "avg/s": int(float(1000 * ops) / duration_ms),
                "max/s": max_throughput
            }
        }
    
    except:
        e, v = sys.exc_info()[:2]
        trace = traceback.format_exc()
        logger.debug(v)
        logger.debug(trace)

        return {
            "result": Result.UNKNOWN
        }
    finally:
        rm("-rf", latency_ok_log_path)
        rm("-rf", latency_err_log_path)
        rm("-rf", throughput_log_path)
        rm("-rf", overview_gnuplot_path)
        rm("-rf", latency_commit_log_path)
        rm("-rf", latency_send_log_path)

def render_availability(config, workload_dir, stat):
    availability_log_path = os.path.join(workload_dir, "availability.log")
    availability_gnuplot_path = os.path.join(workload_dir, "availability.gnuplot")

    try:
        availability_log = open(availability_log_path, "w")

        last_ok = stat.latency_ok_history[0][0]
        for [ts_ms,_] in stat.latency_ok_history:
            availability_log.write(f"{ts_ms}\t{1000 * (ts_ms - last_ok)}\n")
            last_ok = ts_ms
        
        availability_log.close()
        
        with open(availability_gnuplot_path, "w") as gnuplot_file:
            gnuplot_file.write(jinja2.Template(AVAILABILITY).render(
                title = config["name"]))

        gnuplot(availability_gnuplot_path, _cwd=workload_dir)
    except:
        e, v = sys.exc_info()[:2]
        trace = traceback.format_exc()
        logger.debug(v)
        logger.debug(trace)
    finally:
        rm("-rf", availability_log_path)
        rm("-rf", availability_gnuplot_path)

def render_percentiles(config, workload_dir, stat):
    percentiles_log_path = os.path.join(workload_dir, "percentiles.log")
    percentiles_gnuplot_path = os.path.join(workload_dir, "percentiles.gnuplot")

    try:
        percentiles = open(percentiles_log_path, "w")

        latencies = []
        for [_, latency_us] in stat.latency_ok_history:
            latencies.append(latency_us)
        latencies.sort()
        p99  = latencies[int(0.99*len(latencies))]
        for i in range(0,len(latencies)):
            percentiles.write(f"{float(i) / len(latencies)}\t{latencies[i]}\n")
        
        percentiles.close()
        
        with open(percentiles_gnuplot_path, "w") as latency_file:
            latency_file.write(jinja2.Template(LATENCY).render(
                title = config["name"],
                yrange = int(p99*1.2),
                p99 = p99))

        gnuplot(percentiles_gnuplot_path, _cwd=workload_dir)
    except:
        e, v = sys.exc_info()[:2]
        trace = traceback.format_exc()
        logger.debug(v)
        logger.debug(trace)
    finally:
        rm("-rf", percentiles_log_path)
        rm("-rf", percentiles_gnuplot_path)

def collect(config, workload_dir):
    logger.setLevel(logging.DEBUG)
    logger_handler_path = os.path.join(workload_dir, "stat.log")
    handler = logging.FileHandler(logger_handler_path)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(handler)

    check = {
        "result": Result.PASSED
    }
    total = StatInfo()
    for node in config["workload"]["nodes"]:
        node_dir = f"{workload_dir}/{node}"
        if os.path.isdir(node_dir):
            player = LogPlayer(config)

            with open(os.path.join(node_dir, "workload.log"), "r") as workload_file:
                last_line = None
                for line in workload_file:
                    if last_line != None:
                        player.apply(last_line)
                    last_line = line
            
            total.latency_err_history.extend(player.latency_err_history)
            total.latency_ok_history.extend(player.latency_ok_history)
            total.latency_commit_history.extend(player.latency_commit_history)
            total.latency_send_history.extend(player.latency_send_history)

            result = render_overview(config, node_dir, player)
            render_availability(config, node_dir, player)
            render_percentiles(config, node_dir, player)

            check[node] = result
        else:
            check[node] = {
                "result": Result.UNKNOWN,
                "message": f"Can't find logs dir: {workload_dir}"
            }
        check["result"] = Result.more_severe(check["result"], check[node]["result"])
    
    total.latency_err_history.sort(key=lambda x:x[0])
    total.latency_ok_history.sort(key=lambda x:x[0])
    total.latency_commit_history.sort(key=lambda x:x[0])
    total.latency_send_history.sort(key=lambda x:x[0])
    
    check["total"] = render_overview(config, workload_dir, total)
    check["result"] = Result.more_severe(check["result"], check["total"]["result"])
    
    handler.flush()
    handler.close()
    logger.removeHandler(handler)
    if check["result"] == Result.PASSED:
        rm("-rf", logger_handler_path)
    
    return check
