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

public class OpenMeteoService implements WeatherSensor{
    private final Logger logger = LoggerFactory.getLogger(OpenMeteoService.class);

    private final OkHttpClient httpClient;
    private final String BASE_URL;
    private Gson gson;
     
    public OpenMeteoService(){
        this.httpClient = new OkHttpClient();
        this.BASE_URL = "https://api.open-meteo.com/v1/forecast";
        this.gson = new GsonBuilder()
                        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                        .create();
    }

    @Override
    public SensorReadings getReadings() {

        HttpUrl.Builder urlBuilder  = HttpUrl.parse(this.BASE_URL).newBuilder();
        HttpUrl httpUrl = urlBuilder.addQueryParameter("latitude", "52.52")
                                    .addQueryParameter("longitude", "13.41")
                                    .addQueryParameter("current", "temperature_2m,relative_humidity_2m,wind_speed_10m")
                                    .addQueryParameter("temperature_unit", "fahrenheit")
                                    .build();
        Request request = new Request.Builder()
                                    .url(httpUrl)
                                    .get()
                                    .build();
                                    
        SensorReadings readings = new SensorReadings();
        try {
            logger.info("Reading Data...");

            Response response = httpClient.newCall(request).execute();
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
