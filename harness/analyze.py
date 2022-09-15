import os
import json

import sys
from pathlib import Path
from txperf.checks.result import Result
from txperf.workloads.base_money import stat as base_money
from txperf.workloads.tx_money import stat as tx_money
from txperf.workloads.tx_subscribe import stat as tx_subscribe
from txperf.workloads.multi_write import stat as multi_write

suite_report = sys.argv[1]
report_dir = str(Path(suite_report).parent.absolute())

report = None
with open(suite_report, "r") as report_file:
    report = json.load(report_file)

for test_key in report["test_runs"].keys():
    for test_run_key in report["test_runs"][test_key].keys():
        print(f"processing: {test_run_key}")
        
        config = None
        with open(os.path.join(report_dir, test_run_key, "info.json"), "r") as config_file:
            config = json.load(config_file)
        
        if config["workload"]["name"] == "base-money / java":
            try:
                config["workload"]["stat"] = base_money.collect(config, os.path.join(report_dir, test_run_key))
            except:
                config["result"] = Result.more_severe(Result.UNKNOWN, config["result"])
            with open(os.path.join(report_dir, test_run_key, "info.json"), "w") as config_file:
                config_file.write(json.dumps(config, indent=2))
        elif config["workload"]["name"] == "tx-money / java":
            try:
                config["workload"]["stat"] = tx_money.collect(config, os.path.join(report_dir, test_run_key))
            except:
                config["result"] = Result.more_severe(Result.UNKNOWN, config["result"])
            with open(os.path.join(report_dir, test_run_key, "info.json"), "w") as config_file:
                config_file.write(json.dumps(config, indent=2))
        elif config["workload"]["name"] == "tx-subscribe / java":
            try:
                config["workload"]["stat"] = tx_subscribe.collect(config, os.path.join(report_dir, test_run_key))
            except:
                config["result"] = Result.more_severe(Result.UNKNOWN, config["result"])
            with open(os.path.join(report_dir, test_run_key, "info.json"), "w") as config_file:
                config_file.write(json.dumps(config, indent=2))
        elif config["workload"]["name"] == "multi-write / java":
            try:
                config["workload"]["stat"] = multi_write.collect(config, os.path.join(report_dir, test_run_key))
            except:
                config["result"] = Result.more_severe(Result.UNKNOWN, config["result"])
            with open(os.path.join(report_dir, test_run_key, "info.json"), "w") as config_file:
                config_file.write(json.dumps(config, indent=2))
        else:
            raise Exception(f"unknown workflow: {config['workload']['name']}")