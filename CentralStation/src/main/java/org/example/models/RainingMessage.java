package org.example.models;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@Builder
@ToString
public class RainingMessage {
    long stationId;
    int humidity;
}
