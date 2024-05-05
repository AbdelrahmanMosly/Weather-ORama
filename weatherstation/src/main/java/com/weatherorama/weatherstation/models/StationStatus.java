package com.weatherorama.weatherstation.models;

import com.weatherorama.weatherstation.interfaces.Valid;

public class StationStatus implements Valid{
    private long stationId, sNo, statusTimestamp;
    private String batteryStatus;
    private SensorReadings weather;
    
   public StationStatus(long stationID, long sNo, String batteryStatus, SensorReadings weather) {
        this.weather = weather;
        this.stationId = stationID;
        this.sNo = sNo;
        this.batteryStatus = batteryStatus;
        this.statusTimestamp = System.currentTimeMillis();
    }
    public StationStatus() {
       
    }
    public SensorReadings getWeather() {
        return weather;
    }
    public void setWeather(SensorReadings weather) {
        this.weather = weather;
    }
    public long getStationId() {
        return stationId;
    }
    public void setStationId(long stationID) {
        this.stationId = stationID;
    }
    public long getsNo() {
        return sNo;
    }
    public void setsNo(long sNo) {
        this.sNo = sNo;
    }
    public String getBatteryStatus() {
        return batteryStatus;
    }
    public void setBatteryStatus(String batteryStatus) {
        this.batteryStatus = batteryStatus;
    }
    public long getStatusTimestamp() {
        return statusTimestamp;
    }
    public void setStatusTimestamp(long statusTimestamp) {
        this.statusTimestamp = statusTimestamp;
    }
    @Override
    public String toString() {
        return "StationStatus [stationID=" + stationId + ", sNo=" + sNo + ", statusTimestamp=" + statusTimestamp
                + ", batteryStatus=" + batteryStatus + ", weather=" + weather + "]";
    }
    @Override
    public boolean isValid() {
        return weather.isValid();
    }
    
}
