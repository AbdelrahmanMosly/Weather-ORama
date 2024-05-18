package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ParquetToElasticSearchUploader {

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: ParquetToElasticSearchUploader <path_to_parquet_file>");
            System.exit(1);
        }

        String parquetFilePath = args[0];
        String index = getIndexFromPath(parquetFilePath);

        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Parquet to Elasticsearch Uploader")
                .master("local[*]") // Change to your Spark master configuration if needed
                .getOrCreate();

        // Load Parquet file
        Dataset<Row> parquetData = spark.read().parquet(parquetFilePath);

        // Convert Parquet to JSON
        Dataset<String> jsonData = parquetData.toJSON();

        // Elasticsearch REST client setup
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http")).build();

        // Prepare Elasticsearch bulk API request
        StringBuilder bulkRequest = new StringBuilder();
        for (String jsonRecord : jsonData.collectAsList()) {
            bulkRequest.append("{ \"index\" : { \"_index\" : \"" + index + "\" } }\n");
            bulkRequest.append(jsonRecord + "\n");
        }

        // Send bulk request to Elasticsearch
        Request request = new Request("POST", "/_bulk");
        request.setEntity(new NStringEntity(bulkRequest.toString(), ContentType.APPLICATION_JSON));
        Response response = restClient.performRequest(request);

        // Print response status
        System.out.println("Using index name: " + index);
        System.out.println("Using parquet file: " + parquetFilePath);
        System.out.println("Response status: " + response.getStatusLine().getStatusCode());

        // Close SparkSession
        spark.stop();

        // Close the Elasticsearch REST client
        restClient.close();
    }
    private static String getIndexFromPath(String parquetFilePath) {
        Path path = Paths.get(parquetFilePath).getParent();
        return path != null ? path.getFileName().toString() : "default_index";
    }
}
