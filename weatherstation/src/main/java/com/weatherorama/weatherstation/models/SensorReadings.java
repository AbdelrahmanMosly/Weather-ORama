package com.weatherorama.weatherstation.models;

public class SensorReadings{
    private boolean valid;
    private Weather weather;


    public void setWeather(Weather weather) {
        this.weather = weather;
        this.valid = true;
    }

    public Weather getWeather() {
        return weather;
    }

    public boolean isValid(){
        return this.valid;
    }
}
