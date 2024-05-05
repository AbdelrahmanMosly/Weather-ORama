package org.example.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@Builder
@ToString
public class Weather {
    private int humidity;
    private int temperature;
    private int wind_speed;
}
