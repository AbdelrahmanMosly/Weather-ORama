package org.example.archiver;

import org.example.models.WeatherStatus;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;

public class WeatherStatusArchiver {

    //Avro Schema
    public static final Schema SCHEMA= new Schema.Parser().parse("{" +
            "\"type\": \"record\"," +
            "\"name\": \"WeatherStatus\"," +
            "\"fields\": [" +
            "{" +
            "\"name\": \"station_id\"," +
            "\"type\": \"long\"" +
            "}," +
            "{" +
            "\"name\": \"s_no\"," +
            "\"type\": \"long\"" +
            "}," +
            "{" +
            "\"name\": \"battery_status\"," +
            "\"type\": \"string\"" +
            "}," +
            "{" +
            "\"name\": \"status_timestamp\"," +
            "\"type\": \"long\"" +
            "}," +
            "{" +
            "\"name\": \"weather\"," +
            "\"type\": {" +
            "\"type\": \"record\"," +
            "\"name\": \"WeatherInfo\"," +
            "\"fields\": [" +
            "{" +
            "\"name\": \"humidity\"," +
            "\"type\": \"int\"" +
            "}," +
            "{" +
            "\"name\": \"temperature\"," +
            "\"type\": \"int\"" +
            "}," +
            "{" +
            "\"name\": \"wind_speed\"," +
            "\"type\": \"int\"" +
            "}" +
            "]" +
            "}" +
            "}" +
            "]" +
            "}");


    private final Map<Long, List<WeatherStatus>> stationStatusMap;
    private final Map<Long, ParquetWriter<GenericRecord>> stationWriterMap;


    private final static int BATCH_SIZE= 10000;
    private final String outputDirectory;

    public WeatherStatusArchiver(String outputDirectory) throws IOException {
        this.outputDirectory = outputDirectory;
        this.stationStatusMap = new HashMap<>();
        this.stationWriterMap = new HashMap<>();
    }

    public void archiveWeatherStatus(WeatherStatus status) throws IOException {
        long stationId = status.getStationId();
        if (!stationStatusMap.containsKey(stationId)) {
            stationStatusMap.put(stationId, new ArrayList<>());
        }
        stationStatusMap.get(stationId).add(status);
        if (stationStatusMap.get(stationId).size() >= BATCH_SIZE) {
            writeBatch(stationId);
        }
    }

    private void writeBatch(Long stationId) throws IOException {
        List<WeatherStatus> batch = stationStatusMap.get(stationId);
        ParquetWriter<GenericRecord> writer = getWriterForStation(stationId);
        for (WeatherStatus status : batch) {
            GenericRecord record = new GenericData.Record(SCHEMA);
            record.put("station_id", status.getStationId());
            record.put("s_no", status.getSNo());
            record.put("battery_status", status.getBatteryStatus());
            record.put("status_timestamp", status.getStatusTimestamp());
            GenericRecord weatherInfo = new GenericData.Record(SCHEMA.getField("weather").schema());
            weatherInfo.put("humidity", status.getWeather().getHumidity());
            weatherInfo.put("temperature", status.getWeather().getTemperature());
            weatherInfo.put("wind_speed", status.getWeather().getWindSpeed());
            record.put("weather", weatherInfo);
            writer.write(record);
        }
        writer.close();
        stationStatusMap.get(stationId).clear();
    }

    private ParquetWriter<GenericRecord> getWriterForStation(long stationId) throws IOException {
        if (!stationWriterMap.containsKey(stationId)) {
            String stationDirectory = outputDirectory + "/station_" + stationId;
            String fileName = stationDirectory + "/weather_statuses_for_" + stationId + "_" + getCurrentTimestamp() + ".parquet";
            Configuration conf = new Configuration();
            ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(fileName))
                    .withSchema(SCHEMA)
                    .withConf(conf)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withWriteMode(Mode.OVERWRITE)
                    .build();
            stationWriterMap.put(stationId, writer);
        }
        return stationWriterMap.get(stationId);
    }
    private String getCurrentTimestamp() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        return dateFormat.format(new Date());
    }
}
