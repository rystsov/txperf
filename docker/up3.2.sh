#!/usr/bin/env bash

set -e

docker-compose -f docker/docker-compose3.2.yaml --project-directory . up --detach