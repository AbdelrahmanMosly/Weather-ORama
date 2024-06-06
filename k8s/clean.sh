#!/bin/bash

kubectl delete deployment kafka zookeeper central-station weather-station-0 weather-station-1\
         weather-station-2 weather-station-3 weather-station-4 weather-station-5\
         weather-station-6 weather-station-7 weather-station-8 weather-station-9

kubectl delete service central-station-service kafka-service

exit 0