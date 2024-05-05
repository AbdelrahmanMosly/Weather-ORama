package com.weatherorama.weatherstation.models;

public class SensorReadings {
   private int humidity, temperature, windSpeed;
    
    public SensorReadings(int humidity, int temperature, int windSpeed) {
        this.humidity = humidity;
        this.temperature = temperature;
        this.windSpeed = windSpeed;
    }

    public SensorReadings() {}

    public int getHumidity() {
        return humidity;
    }

    public void setHumidity(int humidity) {
        this.humidity = humidity;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    public int getWindSpeed() {
        return windSpeed;
    }

    public void setWindSpeed(int windSpeed) {
        this.windSpeed = windSpeed;
    }

    @Override
    public String toString() {
        return "SensorReadings [humidity=" + humidity + ", temperature=" + temperature + ", windSpeed=" + windSpeed
                + "]";
    } 
    
}
