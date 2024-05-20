#!/bin/bash

cd "$(dirname "$0")"

rm -r weather_stations
mkdir weather_stations

python3 weather_station_generator.py $1


cd weather_stations

for ((i=0; i<$1;i++))
do  
    # echo $i
    kubectl apply -f weather-station-"$i"-deployment.yaml
done
                

exit 0