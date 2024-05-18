FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY ./target/WeatherStation-jar-with-dependencies.jar /app/WeatherStation.jar
COPY ./src/main/resources/log4j2.xml /app/

# ENV PROPS_PATH=/app/app.properties


ENTRYPOINT ["java", "-cp", "WeatherStation.jar", "com.weatherorama.Main"]