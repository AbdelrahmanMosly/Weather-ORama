package org.example.models;

import lombok.*;

import java.io.Serializable;

@Setter
@Getter
@Builder
@ToString
@Data
public class WeatherStatus implements Serializable {
    private long stationId;
    private long sNo;
    private String batteryStatus;
    private long statusTimestamp;
    private Weather weather;

}
