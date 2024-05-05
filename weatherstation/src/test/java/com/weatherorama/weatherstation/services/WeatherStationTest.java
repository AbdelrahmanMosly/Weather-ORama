package com.weatherorama.weatherstation.services;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.weatherorama.centralstation.interfaces.CentralStation;
import com.weatherorama.weatherstation.interfaces.WeatherSensor;
import com.weatherorama.weatherstation.models.SensorReadings;
import com.weatherorama.weatherstation.models.StationStatus;

public class WeatherStationTest {
    private WeatherStation weatherStation;
    private WeatherSensor weatherSensor;
    private CentralStation<Long, StationStatus> centralStation;

    @BeforeEach
    public void setUp() {
        weatherStation = new WeatherStation();
        weatherSensor = mock(WeatherSensor.class);
        centralStation = mock(CentralStation.class);

        weatherStation.setWeatherSensor(weatherSensor);
        weatherStation.setCentralStation(centralStation);
    }

    @Test
    public void testInvoke() {
        SensorReadings readings = new SensorReadings();
        when(weatherSensor.getReadings()).thenReturn(readings);

        weatherStation.invoke();

        verify(weatherSensor, times(1)).getReadings();
        verify(centralStation, times(1)).notify(anyLong(), any(StationStatus.class));
    }

    @Test
    public void testGetBatteryStatus() {
        String batteryStatus = weatherStation.getBatteryStatus();

        assertNotNull(batteryStatus);
        assertTrue(batteryStatus.equals("low") || batteryStatus.equals("medium") || batteryStatus.equals("high"));
    }

    @Test
    public void testGetStationID() {
        long stationID = 12345;
        weatherStation.setStationID(stationID);

        assertEquals(stationID, weatherStation.getStationID());
    }

    @Test
    public void testGetWeatherSensor() {
        assertEquals(weatherSensor, weatherStation.getWeatherSensor());
    }

    @Test
    public void testGetCentralStation() {
        assertEquals(centralStation, weatherStation.getCentralStation());
    }

    @Test
    public void testGetLongitude() {
        double longitude = 1.2345;
        weatherStation.setLongitude(longitude);

        assertEquals(longitude, weatherStation.getLongitude());
    }

    @Test
    public void testGetLatitude() {
        double latitude = 5.6789;
        weatherStation.setLatitude(latitude);

        assertEquals(latitude, weatherStation.getLatitude());
    }
}