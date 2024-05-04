package com.weatherorama.WeatherStation.Services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.WeatherStation.Interfaces.CentralStation;
import com.weatherorama.WeatherStation.Interfaces.WeatherSensor;
import com.weatherorama.WeatherStation.Models.SensorReadings;
import com.weatherorama.WeatherStation.Models.WeatherData;


public class WeatherStation {
    private static Logger logger = LoggerFactory.getLogger((WeatherStation.class));
    private final WeatherSensor weatherSensor;
    private final CentralStation centralStation;

    public WeatherStation(WeatherSensor weatherSensor, CentralStation centralStation){
        this.weatherSensor = weatherSensor;
        this.centralStation = centralStation;
    }

    public void invoke(){
        logger.info("Collecting Data...");
        SensorReadings readings = weatherSensor.getReadings();
        logger.info("Sending Data to Central Station...");
        WeatherData data = new WeatherData();
        centralStation.notify(data);
    }

    
}
