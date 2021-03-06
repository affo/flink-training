#!/bin/bash

if [ -d "./training-dep" ]; then
  echo "The dependency has already been installed"
  echo "Please remove the folder if you want to repeat the process"
  exit 1
fi

git clone https://github.com/dataArtisans/flink-training-exercises.git training-dep
mvn -f ./training-dep/pom.xml clean install

if [ -f "./nycTaxiRides.gz" ]; then
  echo "Rides has already been downloaded"
else
    wget http://training.data-artisans.com/trainingData/nycTaxiRides.gz
fi

if [ -f "./nycTaxiRides.gz" ]; then
  echo "Fares has already been downloaded"
else
    wget http://training.data-artisans.com/trainingData/nycTaxiFares.gz
fi
