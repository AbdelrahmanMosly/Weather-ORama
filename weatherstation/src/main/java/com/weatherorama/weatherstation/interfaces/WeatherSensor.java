package com.weatherorama.weatherstation.interfaces;

import com.weatherorama.weatherstation.models.SensorReadings;

public interface WeatherSensor {
    SensorReadings getReadings();
}
