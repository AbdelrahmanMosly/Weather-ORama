package org.example.models;

import lombok.*;

import java.io.Serializable;

@Setter
@Getter
@Builder
@ToString
@Data
public class Weather implements Serializable {
    private int humidity;
    private int temperature;
    private int windSpeed;
}
