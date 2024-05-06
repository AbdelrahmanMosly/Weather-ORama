package com.weatherorama.weatherstation.models;

public class Weather {
    private int humidity, temperature, windSpeed;

    public Weather(int humidity, int temperature, int windSpeed) {
        this.humidity = humidity;
        this.temperature = temperature;
        this.windSpeed = windSpeed;
    }

    public Weather(OpenMeteoData data){
        this.humidity = data.getRelative_humidity_2m();
        this.temperature = (int)(data.getTemperature_2m() + 0.5);
        this.windSpeed = (int)(data.getWind_speed_10m() + 0.5);
    }

    public int getHumidity() {
        return humidity;
    }

    public int getTemperature() {
        return temperature;
    }

    public int getWindSpeed() {
        return windSpeed;
    }

    
}
