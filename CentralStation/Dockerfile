FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY ./target/CentralStation-jar-with-dependencies.jar /app/CentralStation.jar

# ENV PROPS_PATH=/app/app.properties


ENTRYPOINT ["java", "-cp", "CentralStation.jar", "org.example.CentralStation"]