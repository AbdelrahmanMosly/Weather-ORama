#!/bin/bash

minikube image load weather-station --overwrite=false --daemon=true -v 3
echo "added weather station"
minikube image load central-station --overwrite=false --daemon=true -v 3
echo "added central station"
minikube image load elastic-uploader --overwrite=false --daemon=true -v 3
echo "added elastic uploader"
minikube image load bitnami/kafka:3.4 --overwrite=false --daemon=true -v 3 
echo "added kafka"
minikube image load bitnami/zookeeper:3.9 --overwrite=false --daemon=true -v 3
echo "added zookeeper"
minikube image load nshou/elasticsearch-kibana:kibana7 --overwrite=false --daemon=true -v 3
echo "added elastic-kibana"
