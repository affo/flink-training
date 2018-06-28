#!/bin/bash

if [[ -d "./flink-install" ]] || [ -f "./flink-install.tgz" ]; then
  echo "Flink installation found."
  echo "Please remove the folder and compressed file"
  exit 1
fi

mkdir -p flink-install
cd flink-install
wget http://it.apache.contactlab.it/flink/flink-1.5.0/flink-1.5.0-bin-scala_2.11.tgz -O flink-install.tgz
tar -zxvf flink-install.tgz
