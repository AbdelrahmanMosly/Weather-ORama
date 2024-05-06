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
import java.util.List;

public class WeatherStatusArchiver {

    //Avro Schema
    private static final Schema SCHEMA = new Schema.Parser().parse("{" +
            "\"type\": \"record\"," +
            "\"name\": \"WeatherStatus\"," +
            "\"fields\": [" +
            "{\"name\": \"station_id\", \"type\": \"long\"}," +
            "{\"name\": \"s_no\", \"type\": \"long\"}," +
            "{\"name\": \"battery_status\", \"type\": \"string\"}," +
            "{\"name\": \"status_timestamp\", \"type\": \"long\"}," +
            "{\"name\": \"weather\", \"type\": {" +
            "\"type\": \"record\"," +
            "\"name\": \"WeatherInfo\"," +
            "\"fields\": [" +
            "{\"name\": \"humidity\", \"type\": \"int\"}," +
            "{\"name\": \"temperature\", \"type\": \"int\"}," +
            "{\"name\": \"wind_speed\", \"type\": \"int\"}" +
            "]" +
            "}" +
            "}");

    private ParquetWriter<GenericRecord> writer;
    private int recordCount;

    private final static int RECORDS_THRESHOLD=10000;

    public WeatherStatusArchiver(String outputPath) throws IOException {
        Configuration conf = new Configuration();
        this.writer = AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(outputPath))
                .withSchema(SCHEMA)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(Mode.OVERWRITE)
                .build();
        this.recordCount = 0;
    }

    public void archiveWeatherStatus(List<WeatherStatus> statuses) throws IOException {
        for (WeatherStatus status : statuses) {
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
            recordCount++;

            if (recordCount >= RECORDS_THRESHOLD) {
                writer.close();
                recordCount = 0;
                // Open new Parquet writer for the next batch
                this.writer = AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(getNextFilePath()))
                        .withSchema(SCHEMA)
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withWriteMode(Mode.OVERWRITE)
                        .build();
            }
        }
    }

    private String getNextFilePath() {
        // Implement logic to generate next file path
        // e.g., add timestamp or increment a counter in the file name
        return "path/to/next/file.parquet";
    }

    public void close() throws IOException {
        writer.close();
    }
}
