package com.weatherorama.weatherstation.models;

public class StationStatus {
    private long stationId, sNo, statusTimestamp;
    private String batteryStatus;
    private Weather weather;
    
   public StationStatus(long stationID, long sNo, String batteryStatus, Weather weather) {
        this.weather = weather;
        this.stationId = stationID;
        this.sNo = sNo;
        this.batteryStatus = batteryStatus;
        this.statusTimestamp = System.currentTimeMillis();
    }
    public StationStatus() {
       
    }
    public Weather getWeather() {
        return weather;
    }
    public void setWeather(Weather weather) {
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
}
