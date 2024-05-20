import sys
import os

REPLICAS = "1"
KAFKA_BROKER = "kafka-service:9092"
KAFKA_TOPIC = "weather"
WEATHER_API = "\" \""
RESTART_POLICY = "Always"

class WeatherStation:
    def __init__(self, id,  longitude, latitude, drop_rate=10, poll_every=1000) -> None:
        self.id = str(id)
        self.longitude = str(longitude)
        self.latitude = str(latitude)
        self.drop_rate = str(drop_rate)
        self.poll_every = str(poll_every)

    
    def generate_weather_station_yaml(self, template:str):
        return template.replace("${API_VERSION}", "apps/v1") \
                            .replace("${ID}", self.id) \
                            .replace("${DROP_RATE}", self.drop_rate) \
                            .replace("${POLL_EVERY}", self.poll_every) \
                            .replace("${STATION_LATITUDE}",  self.latitude) \
                            .replace("${STATION_LONGITUDE}",  self.longitude) \
                            .replace("${REPLICAS}", REPLICAS) \
                            .replace("${KAFKA_TOPIC}", KAFKA_TOPIC) \
                            .replace("${KAFKA_BROKER}", KAFKA_BROKER) \
                            .replace("${KAFKA_BROKER}", KAFKA_BROKER) \
                            .replace("${WEATHER_API}", WEATHER_API) \
                            .replace("${RESTART_POLICY}", RESTART_POLICY)



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python weather_station_generator.py <n>")
        sys.exit(1)

    print(sys.argv)
    n = int(sys.argv[1])
    weather_stations = [
        WeatherStation(0, 47.1915, -52.8371),
        WeatherStation(1, 1, 1),
        WeatherStation(2, 2, 2),
        WeatherStation(3, 3, 3),
        WeatherStation(4, 47.1915, -52.8371),
        WeatherStation(5, 5, 5),
        WeatherStation(6, 1, 1),
        WeatherStation(7, 2, 2),
        WeatherStation(8, 3, 3),
        WeatherStation(9, 47.1915, -52.8371),
    ]
    os.chdir(os.path.abspath(os.path.dirname(sys.argv[0])))
    print(os.listdir())

    if(n > len(weather_stations)):
        print(f"Only {weather_stations} will be generated instead of {n}.")

    with open('weather-station-deployment-template.yaml', 'r') as f:
        template = "".join(f.readlines())


    for i in range(min(len(weather_stations), n)):
        with open(f"weather_stations/weather-station-{i}-deployment.yaml", 'w+') as f:
            f.write(weather_stations[i].generate_weather_station_yaml(template))

    


