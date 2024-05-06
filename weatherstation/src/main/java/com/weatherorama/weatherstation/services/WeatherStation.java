package com.weatherorama.weatherstation.services;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.centralstation.interfaces.CentralStation;
import com.weatherorama.weatherstation.interfaces.WeatherSensor;
import com.weatherorama.weatherstation.models.SensorReadings;
import com.weatherorama.weatherstation.models.StationStatus;


/**
 * The WeatherStation class represents a weather station that collects and sends weather data to a central station.
 */
public class WeatherStation {
    private static Logger logger = LoggerFactory.getLogger((WeatherStation.class));
    private Random rng;
    private long currentStatusNo;

    private long stationID;
    private WeatherSensor weatherSensor;
    private CentralStation<Long, StationStatus> centralStation;

    /**
     * Constructs a new WeatherStation object.
     */
    public WeatherStation() {
        this.rng = new Random(System.currentTimeMillis());
    }

    /**
     * Invokes the weather station to collect data and send it to the central station.
     */
    public void invoke(){
        logger.info("Collecting Data...");
        SensorReadings readings = weatherSensor.getReadings();
        if(!readings.isValid()){
            logger.error("Recieved Invalid Reading. The Status won't be sent.");
            return;
        }
        logger.info("Sending Data to Central Station...");

        StationStatus data = new StationStatus(this.stationID, this.currentStatusNo++,
                                                this.getBatteryStatus(), readings.getWeather());
        centralStation.notify(this.stationID, data);
    }

    /**
     * Returns the battery status of the weather station.
     * @return the battery status (low, medium, or high)
     */
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

    /**
     * Returns the station ID of the weather station.
     * @return the station ID
     */
    public long getStationID() {
        return stationID;
    }

    /**
     * Sets the station ID of the weather station.
     * @param stationID the station ID to set
     */
    public void setStationID(long stationID) {
        this.stationID = stationID;
    }

    /**
     * Returns the weather sensor of the weather station.
     * @return the weather sensor
     */
    public WeatherSensor getWeatherSensor() {
        return weatherSensor;
    }

    /**
     * Sets the weather sensor of the weather station.
     * @param weatherSensor the weather sensor to set
     */
    public void setWeatherSensor(WeatherSensor weatherSensor) {
        this.weatherSensor = weatherSensor;
    }

    /**
     * Returns the central station of the weather station.
     * @return the central station
     */
    public CentralStation<Long, StationStatus> getCentralStation() {
        return centralStation;
    }

    /**
     * Sets the central station of the weather station.
     * @param centralStation the central station to set
     */
    public void setCentralStation(CentralStation<Long, StationStatus> centralStation) {
        this.centralStation = centralStation;
    }
}
