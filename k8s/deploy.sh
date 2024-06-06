#!/bin/bash

cd "$(dirname "$0")"
cd services
echo "Creating Kafka Service..."
./service_kafka.sh

echo "Creating Central Station Service..."
./service_central_station.sh

cd ../deployments

echo "Deploying Kafka..."
./deploy_kafka.sh

echo "Deploying Central Station..."
./deploy_central_station.sh

echo "Deploying Weather Stations..."
./deploy_weather_stations.sh $1

echo "Exposing Central Station..."
minikube service central-station-service
