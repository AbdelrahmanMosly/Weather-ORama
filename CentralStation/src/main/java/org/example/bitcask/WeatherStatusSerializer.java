package org.example.bitcask;

import org.example.models.WeatherStatus;

import java.io.*;

public class WeatherStatusSerializer {
    public static byte[] serialize(WeatherStatus weatherStatus) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            // Serialize WeatherStatus object
            objectOutputStream.writeObject(weatherStatus);
            return byteArrayOutputStream.toByteArray();
        }
    }

    public static WeatherStatus deserialize(byte[] data) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            // Deserialize byte array into WeatherStatus object
            return (WeatherStatus) objectInputStream.readObject();
        }
    }
}
