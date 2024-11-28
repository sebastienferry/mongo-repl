#!/bin/bash

# Check if the volume exists
if ! docker volume ls | grep -q prometheus_data; then
  echo "Creating the prometheus_data volume"
  docker volume create prometheus_data
fi

# Run the prometheus container
docker-compose -f ../tools/prometheus/docker-compose.yml up -d