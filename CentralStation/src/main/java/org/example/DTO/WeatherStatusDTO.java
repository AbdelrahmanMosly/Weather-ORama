package org.example.DTO;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.example.entity.Weather;
import org.example.entity.WeatherStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class WeatherStatusDTO {
    private final ObjectMapper mapper = new ObjectMapper();

    public WeatherStatus deserialize(byte[] message) throws IOException {
        try {
            return mapper.readValue(message, WeatherStatus.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    final Schema schema;
//
//    public WeatherStatusDTO(String schemaFilePath) {
//        String avroSchemaString = "";
//        try {
//            byte[] schemaBytes = Files.readAllBytes(Paths.get(schemaFilePath));
//            avroSchemaString = new String(schemaBytes, StandardCharsets.UTF_8);
//        } catch (IOException e) {
//            avroSchemaString = "{\"type\":\"record\",\"name\":\"WeatherStatus\",\"fields\":[{\"name\":\"station_id\",\"type\":\"long\"},{\"name\":\"s_no\",\"type\":\"long\"},{\"name\":\"battery_status\",\"type\":\"string\"},{\"name\":\"status_timestamp\",\"type\":\"long\"},{\"name\":\"weather\",\"type\":{\"type\":\"record\",\"name\":\"Weather\",\"fields\":[{\"name\":\"humidity\",\"type\":\"int\"},{\"name\":\"temperature\",\"type\":\"int\"},{\"name\":\"wind_speed\",\"type\":\"int\"}]}}]}";
//        }
//        schema = new Schema.Parser().parse(avroSchemaString);
//
//    }
//
//    public WeatherStatus deserialize(byte[] message) throws IOException {
//        if (message == null) {
//            return null;
//        } else {
//            Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
//            DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
//            GenericRecord avro = datumReader.read(null, decoder);
//            GenericRecord avroWeather = ((GenericRecord) avro.get("weather"));
//
//            Weather weather = Weather.builder()
//                    .humidity((int) avroWeather.get("humidity"))
//                    .temperature((int) avroWeather.get("temperature"))
//                    .wind_speed((int) avroWeather.get("wind_speed"))
//                    .build();
//
//            return WeatherStatus.builder()
//                    .station_id((Long) avro.get("station_id"))
//                    .s_no((Long) avro.get("s_no"))
//                    .battery_status((String) avro.get("battery_status").toString())
//                    .status_timestamp((long) avro.get("status_timestamp"))
//                    .weather(weather)
//                    .build();
//        }
//    }

}
