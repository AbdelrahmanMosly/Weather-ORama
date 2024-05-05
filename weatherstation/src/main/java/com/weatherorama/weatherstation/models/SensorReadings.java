package com.weatherorama.weatherstation.models;

import com.weatherorama.weatherstation.interfaces.Valid;

public class SensorReadings implements Valid{
    private int humidity, temperature, windSpeed;
    private transient boolean valid;
    
    public SensorReadings(int humidity, int temperature, int windSpeed) {
        this.humidity = humidity;
        this.temperature = temperature;
        this.windSpeed = windSpeed;
        this.valid = true;
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

    public void load(OpenMeteoResponse openMeteoResponse) {
        this.humidity = openMeteoResponse.getCurrent().getRelative_humidity_2m();
        this.temperature = (int) (openMeteoResponse.getCurrent().getTemperature_2m() + 0.5);
        this.windSpeed = (int) (openMeteoResponse.getCurrent().getWind_speed_10m() + 0.5);
        this.valid = true;
    }

    @Override
    public boolean isValid() {
        return valid;
    } 
    
}
