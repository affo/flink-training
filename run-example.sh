#!/bin/bash
if [[ "$#" -lt 1 ]]; then
  echo "Provide main class please"
  exit 1
fi

args="${@:2}"

if [[ -z "$FLINK_HOME" ]]; then
  echo "No FLINK_HOME set, using mvn exec plugin"
  mvn -f ./transformations/pom.xml exec:java -Padd-dependencies-for-IDEA \
    -Dexec.mainClass="affetti.flink.examples.$1" -Dexec.args="$args"
else
  java -cp $FLINK_HOME/lib/*:./transformations/target/transformations.jar \
    affetti.flink.examples.$1 $args
fi
