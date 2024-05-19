package org.example.services;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.example.models.WeatherStatus;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class RainProcessor implements Processor<String, String, String, String>{
    private final Gson gson;
    private ProcessorContext<String, String> context;
    private final String rainingSinkName;
    private final int humidityThreshold;

    public RainProcessor(String rainingSinkName, int humidityThreshold) {
        this.rainingSinkName = rainingSinkName;
        this.humidityThreshold = humidityThreshold;
        

        this.gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, String> record) {
        WeatherStatus weatherStatus = this.gson.fromJson(record.value(), WeatherStatus.class);
        if (weatherStatus.getWeather().getHumidity() > this.humidityThreshold) {
            System.out.println("It's gonna rain!");
            this.context.forward(new Record<String, String>(record.key(),
                                                            "It's raining Tacos.",
                                                            System.currentTimeMillis()),
                                    rainingSinkName);
        }else{
            System.out.println("It's not gonna rain!");
        }
        
    }

    
}
