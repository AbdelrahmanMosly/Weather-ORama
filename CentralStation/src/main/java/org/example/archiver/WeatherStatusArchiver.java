package org.example.archiver;

import org.example.models.WeatherStatus;

import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
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


    private final int batchSize;
    private final String outputDirectory;
    private final ExecutorService executorService;

    public WeatherStatusArchiver(String outputDirectory, int batchSize) throws IOException {
        this.outputDirectory = outputDirectory;
        this.stationStatusMap = new HashMap<>();
        this.executorService = Executors.newCachedThreadPool();
        this.batchSize = batchSize;
    }

    public void archiveWeatherStatus(WeatherStatus status) throws IOException {
        log.debug("=====================================================");
        // log.debug("Archiving weather status: {}", status);
        long stationId = status.getStationId();
        if (!stationStatusMap.containsKey(stationId)) {
            stationStatusMap.put(stationId, new ArrayList<>());
        }
        stationStatusMap.get(stationId).add(status);
        log.debug("Station {} has {} statuses", stationId, stationStatusMap.get(stationId).size());
        log.debug("=====================================================");

        if (stationStatusMap.get(stationId).size() >= batchSize) {
            writeBatch(stationId);
        }
    }

    private void writeBatch(Long stationId) throws IOException {
        List<WeatherStatus> batch = stationStatusMap.remove(stationId);
        executorService.submit(() -> {
            try (ParquetWriter<GenericRecord> writer = getWriterForStation(stationId);){
            
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
                    try{
                        log.debug("Writing record: {}", record);
                        writer.write(record);
                        log.debug("Wrote record: {}", record);   
                    }catch (IOException e) {
                        log.debug("Error writing record for station {}", stationId);
                        e.printStackTrace();
                    }
                }

            }catch (IOException e) {
                log.debug("Error creating writer for station {}", stationId);
                e.printStackTrace();
            }
        });
    }

    private ParquetWriter<GenericRecord> getWriterForStation(long stationId) throws IOException {
        String stationDirectory = outputDirectory + "/station_" + stationId;
        String fileName = stationDirectory + "/weather_statuses_for_" + stationId + "_" + getCurrentTimestamp() + ".parquet";
        Configuration conf = new Configuration();
        Path path = new Path(fileName);
        return AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(path, conf))
                                .withSchema(SCHEMA)
                                .withConf(conf)
                                // .withCompressionCodec(CompressionCodecName.SNAPPY)
                                .withWriteMode(Mode.OVERWRITE)
                                .withValidation(false)
                                .build();
            // try {

               
            // } catch (IOException e) {
            //     log.debug("Error creating writer for station {}", stationId);
            //     log.debug(e.toString());
            //     throw new RuntimeException(e); // Rethrow as unchecked exception
            // }
        // }
    }
    private String getCurrentTimestamp() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        return dateFormat.format(new Date());
    }
}
