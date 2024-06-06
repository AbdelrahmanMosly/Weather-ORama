#!/bin/bash


mvn clean
mvn package -DskipTests

docker build -t elastic-uploader:latest -f elasticsearch-uploader-dockerfile .