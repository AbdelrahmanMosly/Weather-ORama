package com.weatherorama.weatherstation.services;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.centralstation.interfaces.CentralStation;
import com.weatherorama.weatherstation.interfaces.WeatherSensor;
import com.weatherorama.weatherstation.models.SensorReadings;
import com.weatherorama.weatherstation.models.StationStatus;


public class WeatherStation {
    private static Logger logger = LoggerFactory.getLogger((WeatherStation.class));
    private Random rng;
    private long currentStatusNo;

    private long stationID;
    private WeatherSensor weatherSensor;
    private CentralStation<Long, StationStatus> centralStation;
    private double longitude, latitude;
    
    // public WeatherStation(long stationID, WeatherSensor weatherSensor,
    //                         CentralStation<Long, StationStatus> centralStation){
    //     this.stationID = stationID;
    //     this.weatherSensor = weatherSensor;
    //     this.centralStation = centralStation;
    //     this.rng = new Random(System.currentTimeMillis());
    // }

    public WeatherStation() {
        this.rng = new Random(System.currentTimeMillis());
    }

    public void invoke(){
        logger.info("Collecting Data...");
        SensorReadings readings = weatherSensor.getReadings();
        logger.info("Sending Data to Central Station...");

        StationStatus data = new StationStatus(this.stationID, this.currentStatusNo++,
                                                this.getBatteryStatus(), readings);
        centralStation.notify(this.stationID, data);
    }

    String getBatteryStatus(){
        int rn = this.rng.nextInt(10);
        String batteryStatus = "medium";
        if(rn < 3){
            batteryStatus = "low";
        }else if(rn < 6){
            batteryStatus = "high";
        }
        return batteryStatus;
    }

    public long getStationID() {
        return stationID;
    }

    public void setStationID(long stationID) {
        this.stationID = stationID;
    }

    public WeatherSensor getWeatherSensor() {
        return weatherSensor;
    }

    public void setWeatherSensor(WeatherSensor weatherSensor) {
        this.weatherSensor = weatherSensor;
    }

    public CentralStation<Long, StationStatus> getCentralStation() {
        return centralStation;
    }

    public void setCentralStation(CentralStation<Long, StationStatus> centralStation) {
        this.centralStation = centralStation;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }
    

    
}
