#!/usr/bin/env bash

set -e

control=$(ansible-inventory -i hosts.ini --list | jq -r ".control.hosts[0]")

fetch_logs () {
    ssh -i ./id_ed25519 ubuntu@$control tar -czf /mnt/vectorized/experiments.tar.gz /mnt/vectorized/experiments
    scp -i ./id_ed25519 ubuntu@$control:/mnt/vectorized/experiments.tar.gz .
    tar -xzf experiments.tar.gz
    mkdir -p $1
    mv mnt/vectorized/experiments/* $1/
    rm -rf mnt
    ssh -i ./id_ed25519 ubuntu@$control rm /mnt/vectorized/experiments.tar.gz
    ssh -i ./id_ed25519 ubuntu@$control rm -rf '/mnt/vectorized/experiments/*'
    rm experiments.tar.gz
}

echo "$(date) test_suite_tx_money_64_redpanda" >> log
ansible-playbook playbooks/test.suite.yml --key-file id_ed25519 -e suite_path=test_suite_tx_money_64_redpanda.json -e repeat=3
fetch_logs "results/tx-money-redpanda-64"

echo "$(date) test_suite_tx_money_64_kafka" >> log
ansible-playbook playbooks/test.suite.yml --key-file id_ed25519 -e suite_path=test_suite_tx_money_64_kafka.json -e repeat=3
fetch_logs "results/tx-money-kafka-64"

echo "$(date) test_suite_base_money_64_redpanda" >> log
ansible-playbook playbooks/test.suite.yml --key-file id_ed25519 -e suite_path=test_suite_base_money_64_redpanda.json -e repeat=3
fetch_logs "results/base-money-redpanda-64"

echo "$(date) test_suite_base_money_64_kafka" >> log
ansible-playbook playbooks/test.suite.yml --key-file id_ed25519 -e suite_path=test_suite_base_money_64_kafka.json -e repeat=3
fetch_logs "results/base-money-kafka-64"