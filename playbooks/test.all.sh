#!/usr/bin/env bash

set -e

if [ "$AWS_CHAOS_RESOURCE_PREFIX" == "" ]; then
    echo "set AWS_CHAOS_RESOURCE_PREFIX to distinguish aws resources between users"
    exit 1
fi

if [ ! -f id_ed25519 ]; then
    ssh-keygen -t ed25519 -f id_ed25519 -N ""
fi

terraform apply -var="username=$AWS_CHAOS_RESOURCE_PREFIX" -var="redpanda_cluster_size=3" -var="workload_cluster_size=2" -auto-approve
sleep 1m

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

ansible-playbook deploy.yml --key-file id_ed25519

echo "$(date) test_suite_tx_money_64_redpanda" >> log
ansible-playbook playbooks/test.suite.yml --key-file id_ed25519 -e suite_path=test_suite_tx_money_64_redpanda.json
fetch_logs "results/tx-money-redpanda-64"

echo "$(date) test_suite_tx_money_64_kafka" >> log
ansible-playbook playbooks/test.suite.yml --key-file id_ed25519 -e suite_path=test_suite_tx_money_64_kafka.json
fetch_logs "results/tx-money-kafka-64"

echo "$(date) test_suite_base_money_64_redpanda" >> log
ansible-playbook playbooks/test.suite.yml --key-file id_ed25519 -e suite_path=test_suite_base_money_64_redpanda.json
fetch_logs "results/base-money-redpanda-64"

echo "$(date) test_suite_base_money_64_kafka" >> log
ansible-playbook playbooks/test.suite.yml --key-file id_ed25519 -e suite_path=test_suite_base_money_64_kafka.json
fetch_logs "results/base-money-kafka-64"

terraform destroy -var="username=$AWS_CHAOS_RESOURCE_PREFIX" -var="redpanda_cluster_size=3" -var="workload_cluster_size=2" -auto-approve