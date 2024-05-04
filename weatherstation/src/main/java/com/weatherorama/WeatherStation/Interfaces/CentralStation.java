package com.weatherorama.WeatherStation.Interfaces;

import com.weatherorama.WeatherStation.Models.WeatherData;

public interface CentralStation {
    void notify(WeatherData data);
}
