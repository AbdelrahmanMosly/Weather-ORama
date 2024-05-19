FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY ./target/CentralStation.jar /app/CentralStation.jar
COPY ./src/main/resources/logback.xml /app/logback.xml

# ENV PROPS_PATH=/app/app.properties


ENTRYPOINT ["java", "-cp", "CentralStation.jar", "org.example.CentralStation"]