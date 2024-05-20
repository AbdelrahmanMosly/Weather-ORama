#!/bin/bash

mvn clean
# mvn dependency:copy-dependencies -DincludeScope=runtime
mvn package -DskipTests

docker build -t central-station:jfr .