package com.weatherorama.centralstation.interfaces;

import com.weatherorama.weatherstation.models.WeatherData;

public interface CentralStation {
    void notify(WeatherData data);
}
