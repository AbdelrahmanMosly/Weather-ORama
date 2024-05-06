package com.weatherorama.centralstation.interfaces;

public interface CentralStation <K, V>{
    void notify(K id, V data);
}
