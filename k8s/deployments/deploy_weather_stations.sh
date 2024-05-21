#!/bin/bash

cd "$(dirname "$0")"

rm -r weather_stations
mkdir weather_stations

n=${1:-10}
n=$(($n > 10 ? 10 : $n))

python3 weather_station_generator.py $n


cd weather_stations

for ((i=0; i<$n;i++))
do  
    # echo $i
    kubectl apply -f weather-station-"$i"-deployment.yaml
done
                

exit 0