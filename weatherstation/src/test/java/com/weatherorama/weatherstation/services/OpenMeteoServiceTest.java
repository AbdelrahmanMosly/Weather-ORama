package com.weatherorama.weatherstation.services;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.weatherorama.weatherstation.models.SensorReadings;

public class OpenMeteoServiceTest {
    @Test
    void testGetReadings() {
        // Create an instance of OpenMeteoService
        OpenMeteoService openMeteoService = new OpenMeteoService();

        // Call the getReadings() method
        SensorReadings readings = openMeteoService.getReadings();

        // Assert that the readings object is not null
        assertNotNull(readings);

        assertTrue(readings.getTemperature() >= -51 && readings.getTemperature() <= 120);
        assertTrue(readings.getHumidity() >= 0 && readings.getHumidity() <= 100);
        assertTrue(readings.getWindSpeed() >= 0 && readings.getWindSpeed() <= 1000);
    }
}
