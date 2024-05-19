package com.weatherorama.weatherstation.services;

import com.weatherorama.weatherstation.interfaces.WeatherSensor;
import com.weatherorama.weatherstation.mocks.MockWeatherSensor;

public class WeatherSensorFactory {

    public static WeatherSensor getWeatherSensor(String weatherAPI, double longitude, double latitude) {
        return weatherAPI.isBlank()? new MockWeatherSensor():
                                     new OpenMeteoService(weatherAPI, longitude, latitude);
    }

}
