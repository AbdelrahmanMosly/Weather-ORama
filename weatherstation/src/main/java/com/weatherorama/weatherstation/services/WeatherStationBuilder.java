package com.weatherorama.weatherstation.services;

import com.weatherorama.centralstation.interfaces.CentralStation;
import com.weatherorama.weatherstation.interfaces.WeatherSensor;
import com.weatherorama.weatherstation.models.StationStatus;

public class WeatherStationBuilder {
    WeatherStation weatherStation;

    public WeatherStationBuilder(){
        this.weatherStation = new WeatherStation();
    }

    public WeatherStationBuilder stationId(long stationId){
        this.weatherStation.setStationID(stationId);
        return this;
    }

    public WeatherStationBuilder longitude(double longitude){
        this.weatherStation.setLongitude(longitude);
        return this;
    }

    public WeatherStationBuilder latitude(double latitude){
        this.weatherStation.setLatitude(latitude);
        return this;
    }

    public WeatherStationBuilder centralStation(CentralStation<Long, StationStatus> centralStation){
        this.weatherStation.setCentralStation(centralStation);
        return this;
    }

    public WeatherStationBuilder weatherSensor(WeatherSensor weatherSensor){
        this.weatherStation.setWeatherSensor(weatherSensor);
        return this;
    }

    public WeatherStation build(){
        WeatherStation product = this.weatherStation;
        this.weatherStation = new WeatherStation();
        return product;
    }
    
}
