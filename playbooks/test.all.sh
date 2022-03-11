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

echo "predeploy"
ansible-playbook predeploy.yml --key-file id_ed25519
echo "deploy"
ansible-playbook playbooks/control.deploy.yml --key-file id_ed25519
echo "execute tests"
ansible-playbook playbooks/control.execute.tests.yml --key-file id_ed25519

ssh -i ./id_ed25519 ubuntu@$control tar -czf /mnt/vectorized/txperf/results.tar.gz -C /mnt/vectorized/txperf results
scp -i id_ed25519 -r ubuntu@$control:/mnt/vectorized/txperf/results.tar.gz results.tar.gz

terraform destroy -var="username=$AWS_CHAOS_RESOURCE_PREFIX" -var="redpanda_cluster_size=3" -var="workload_cluster_size=2" -auto-approve