package com.weatherorama.weatherstation.services;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import com.weatherorama.weatherstation.interfaces.WeatherSensor;
import com.weatherorama.weatherstation.models.OpenMeteoResponse;
import com.weatherorama.weatherstation.models.SensorReadings;

/**
 * A service class that implements the WeatherSensor interface and provides weather readings using the OpenMeteo API.
 */
public class OpenMeteoService implements WeatherSensor{
    private final Logger logger = LoggerFactory.getLogger(OpenMeteoService.class);

    private final OkHttpClient httpClient;
    private Gson gson;

    private final Request request;
     
    /**
     * Constructs a new OpenMeteoService object.
     * Initializes the OkHttpClient, base URL, and Gson object.
     */
    public OpenMeteoService(String base_url, double longitude, double latitude){
        this.httpClient = new OkHttpClient();
                       
        HttpUrl.Builder urlBuilder  = HttpUrl.parse(base_url).newBuilder();

        HttpUrl httpUrl = urlBuilder.addQueryParameter("latitude", String.valueOf(latitude))
                                    .addQueryParameter("longitude", String.valueOf(longitude))
                                    .addQueryParameter("current", "temperature_2m,relative_humidity_2m,wind_speed_10m")
                                    .addQueryParameter("temperature_unit", "fahrenheit")
                                    .build();

        this.request = new Request.Builder()
                                    .url(httpUrl)
                                    .get()
                                    .build();
        this.gson = new GsonBuilder()
                        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                        .create();
    }

    @Override
    public SensorReadings getReadings() {
        SensorReadings readings = new SensorReadings();
        try {
            logger.info("Reading Data...");

            Response response = httpClient.newCall(this.request).execute();
            if(response.isSuccessful()){
                logger.info("Data was read successfuly.");
                String responseBody = response.body().string();
                logger.debug(responseBody);
                OpenMeteoResponse mappedResponse = this.gson.fromJson(responseBody, OpenMeteoResponse.class);
                readings.load(mappedResponse);

            }else{
                logger.info("An error occured while reading the data.");
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return readings;
    }
    
}
