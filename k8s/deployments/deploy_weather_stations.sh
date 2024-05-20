#!/bin/bash

kubectl apply -f weather-station-0-deployment.yaml
kubectl apply -f weather-station-1-deployment.yaml
kubectl apply -f weather-station-2-deployment.yaml
kubectl apply -f weather-station-3-deployment.yaml
kubectl apply -f weather-station-4-deployment.yaml
                