import sys
import os

REPLICAS = "1"
KAFKA_BROKER = "kafka-service:9092"
KAFKA_TOPIC = "weather"
WEATHER_API = "https://api.open-meteo.com/v1/forecast"
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

    n = int(sys.argv[1])
    weather_stations = [
        WeatherStation(0, 47.1915, -52.8371),
        WeatherStation(1, 31.2065, 29.9249),
        WeatherStation(2, 31.6100, 25.9248),
        WeatherStation(3, 30.0232, 31.2351),
        WeatherStation(4, 31.2558, 32.2929),
        WeatherStation(5, 30.0690, 31.3121),
        WeatherStation(6, 48.8415, 2.2531),
        WeatherStation(7, 51.4926, 7.4518),
        WeatherStation(8, 40.45304, -3.6883),
        WeatherStation(9, 51.55604, -0.2796),
    ]
    os.chdir(os.path.abspath(os.path.dirname(sys.argv[0])))

    if(n > len(weather_stations)):
        print(f"Only {weather_stations} will be generated instead of {n}.")

    with open('weather-station-deployment-template.yaml', 'r') as f:
        template = "".join(f.readlines())


    for i in range(min(len(weather_stations), n)):
        with open(f"weather_stations/weather-station-{i}-deployment.yaml", 'w+') as f:
            f.write(weather_stations[i].generate_weather_station_yaml(template))

    


