FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

ENV MAX_OBJECTS_PER_SEGMENT=128
ENV COMPACT_INTERVAL=60000
ENV SNAPSHOT_INTERVAL=300000
ENV PARQUET_DIR=parquet
ENV BITCASK_DIR=bitcask


COPY ./target/CentralStation.jar /app/CentralStation.jar
COPY ./src/main/resources/logback.xml /app/logback.xml

# ENV PROPS_PATH=/app/app.properties


ENTRYPOINT ["java", "-cp", "CentralStation.jar", "org.example.CentralStation"]