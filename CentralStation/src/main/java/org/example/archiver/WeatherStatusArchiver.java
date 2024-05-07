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

    private final static int BATCH_SIZE= 10000;
    private final String outputDirectory;

    public WeatherStatusArchiver(String outputDirectory) throws IOException {
        this.outputDirectory = outputDirectory;
        Configuration conf = new Configuration();
        this.writer = AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(getNextFilePath()))
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
        System.out.println( "records not written:" +recordCount);
        recordCount++;
        if (recordCount >= BATCH_SIZE) {
            writeBatch();
        }
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
        resetWriterAndBatch();
    }

    private void resetWriterAndBatch() throws IOException {
        recordCount = 0;
        batch.clear();
        writer.close();
        System.out.println("records written:" + recordCount);
        // Open new Parquet writer for the next batch
        this.writer = AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(getNextFilePath()))
                .withSchema(SCHEMA)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(Mode.OVERWRITE)
                .build();
    }


    private String getNextFilePath() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = dateFormat.format(new Date());
        String fileName = "/weather_statuses_batch_" + batchIndex + "_" + timestamp + ".parquet";
        batchIndex++;
        return outputDirectory + fileName;
    }
}
