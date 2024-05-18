FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY ./target/WeatherStation-jar-with-dependencies.jar /app/WeatherStation.jar
COPY ./src/main/resources/* /app/

ENV PROPS_PATH=/app/app.properties


ENTRYPOINT ["/bin/sh"]