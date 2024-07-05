# Weather Stations Monitoring

## Overview

The Weather Stations Monitoring project aims to efficiently process and analyze high-frequency data streams from distributed weather stations within the Internet of Things (IoT) context. This project was developed as part of the course "Designing Data Intensive Applications."

## Team Members
- [Abdelrahman Mosly](https://github.com/AbdelrahmanMosly)
- [Abdelrahman Gad](https://github.com/abdelrahman-gad-alex)
- [Karim Alaa](https://github.com/Karim19Alaa)
## Problem Statement

The project addresses the challenge of efficiently processing and analyzing high-frequency data streams from distributed weather stations. It involves designing and implementing a robust system architecture comprising data acquisition, processing, archiving, and indexing stages. Additionally, the system integrates with the Open-Meteo API to enhance data sources. The primary goal is to develop a scalable and reliable weather monitoring system capable of handling diverse data types, ensuring data integrity, and enabling advanced analytics for weather forecasting and analysis.

## Implementation

### Weather Station Integration

- Integrated with the Open-Meteo API to simulate real-time weather data.
- The Weather Station mock fetches weather information such as temperature, humidity, and wind speed for specified locations.
- Data is periodically sent to the Central Station system for storage and retrieval, mimicking the behavior of actual weather stations.

### Kafka Connection

- Used Bitnami images for Kafka and Zookeeper to create corresponding containers.
- Weather Stations use Kafka Producer API to send readings and station status on the "weather" topic.
- Central Station utilizes the Consumer API to consume messages stored in the corresponding topic.

### Raining Triggers in Kafka Process

- Used Kafka’s Processor API to process arriving records at the "weather" topic.
- Sent a special message to the "rain" topic indicating it’s raining if the humidity level is above 70%.

### Base Central Station

- The core component responsible for processing and archiving weather data received from multiple weather stations.
- Utilizes Bitcask for data storage, archiving data into Parquet files, and potentially indexing in Elasticsearch for further analysis.
- Manages the creation of Kafka topics for weather data and rain alerts.

### BitCask Riak

- Manages data segment files, periodically compacts segments to save storage, and takes snapshots for quick recovery.

### Parquet-files Archiving

- Converts stored weather data into Parquet format for efficient storage and querying.
- Uses a watch script to monitor Parquet files and triggers a jar file to ingest data into Elasticsearch for real-time indexing and analysis.

### Deployment Using Kubernetes

- Created Docker images for weather stations, central station, and Elasticsearch uploader.
- Used Kubernetes for deploying components, including weather stations, Zookeeper, Kafka broker, Central Station, Elasticsearch uploader, Elasticsearch, and Kibana.
- Implemented shared storage for saving Parquet files and BitCask entries.

## Enterprise Integration Patterns

### Data Flow in Weather Station

![Data Flow in Weather Station](https://github.com/AbdelrahmanMosly/Weather-ORama/assets/95633556/bb8d0df9-e5a5-423a-b584-90079df11c37)


### Data Flow throughout Kafka

![Data Flow throughout Kafka](https://github.com/AbdelrahmanMosly/Weather-ORama/assets/95633556/79f95076-ca65-47b5-94b1-bdcae51c7f76)


### Data Flow in Central Station

![Data Flow in Central Station](https://github.com/AbdelrahmanMosly/Weather-ORama/assets/95633556/40c7995f-55fb-44f8-920a-b4529efd28a3)


## Running The System

For a faster experience, the Central Station waits for only 100 records per station before archiving them into Parquet files.

## Kibana’s Dashboard

- Queries used to get dropped messages percentage and low battery percentage:
  ```kql
  1 - count() / max(s_no)
  count(kql='battery_status.keyword == 'low') / count()
  ```
![image](https://github.com/AbdelrahmanMosly/Weather-ORama/assets/95633556/055a1754-2852-41e2-96d7-f9172c9e6c4d)

## Performance Diagnostics using JFR

- Attached JFR to the Central Station JAR for 10 minutes to diagnose performance.
  ![image](https://github.com/AbdelrahmanMosly/Weather-ORama/assets/95633556/5f1c46a8-3869-49f5-8ae8-b3114690c615)
  ![image](https://github.com/AbdelrahmanMosly/Weather-ORama/assets/95633556/4d1555bc-e94a-44b4-b2d7-01b527b2a7fe)
  ![image](https://github.com/AbdelrahmanMosly/Weather-ORama/assets/95633556/e956cfa7-462a-45df-bdb7-7674210e67d0)


