package com.weatherorama.centralstation.interfaces;

import com.weatherorama.weatherstation.models.StationStatus;

public interface CentralStation {
    void notify(StationStatus data);
}
