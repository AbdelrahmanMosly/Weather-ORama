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
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;
import java.util.Date;

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


    private ParquetWriter<GenericRecord> writer;
    private final List<WeatherStatus> batch;

    private int recordCount;
    private int batchIndex;

    private final static int BATCH_SIZE=10000;

    public WeatherStatusArchiver(String outputPath) throws IOException {
        Configuration conf = new Configuration();
        this.writer = AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(outputPath))
                .withSchema(SCHEMA)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(Mode.OVERWRITE)
                .build();
        this.batch = new ArrayList<>();
        this.recordCount = 0;
    }

    public void archiveWeatherStatus(WeatherStatus status) throws IOException {
        batch.add(status);
        recordCount++;
        if (recordCount >= BATCH_SIZE) {
            writeBatch();
        }
    }

    private GenericRecord convertToGenericRecord(WeatherStatus status) {
        // Convert WeatherStatus object to GenericRecord
        // This code snippet should be replaced with your actual conversion logic
        return null;
    }
    private void writeBatch() throws IOException {
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
        batch.clear();
        recordCount = 0;
    }

    public void close() throws IOException {
        if (!batch.isEmpty()) {
            writeBatch();
        }
        writer.close();
    }

    private String getNextFilePath() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = dateFormat.format(new Date());
        String fileName = "weather_statuses_batch_" + batchIndex + "_" + timestamp + ".parquet";
        batchIndex++;
        return "~/parquet-files/" + fileName;
    }
}
