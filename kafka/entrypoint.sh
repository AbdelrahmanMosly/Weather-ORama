#! /bin/sh
kafka-topics.sh --bootstrap-server ${KAFKA} --list
# Create weather and rain topic
kafka-topics.sh --bootstrap-server ${KAFKA} --create --if-not-exists --topic ${WEATHER_TOPIC}
kafka-topics.sh --bootstrap-server ${KAFKA} --create --if-not-exists --topic ${RAIN_TOPIC} 

kafka-topics.sh --bootstrap-server ${KAFKA} --list