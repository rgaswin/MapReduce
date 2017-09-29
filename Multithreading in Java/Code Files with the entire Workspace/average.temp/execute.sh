#!/bin/bash

mvn clean package

java -jar target/average.temp-0.0.1-SNAPSHOT.jar
