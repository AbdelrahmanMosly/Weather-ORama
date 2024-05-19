package org.example.models;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@Builder
@ToString
public class WeatherStatus {
    private long stationId;
    private long sNo;
    private String batteryStatus;
    private long statusTimestamp;
    private Weather weather;

}
