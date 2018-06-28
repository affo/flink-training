# SP lectures

## For the examples from official Flink training

Follow the instructions [here](flink-training/README.md).

## Deploy in cluster with Docker for fault tolerance experiment

The `docker-compose.yml` file provides you with Kafka, Influx and Grafana. The
Flink job can be run by downloading a Flink installation and running the cluster
manually (more
[here](https://ci.apache.org/projects/flink/flink-docs-release-1.5/quickstart/setup_quickstart.html)).

In order to run the job:

 * open Flink's web dashboard available at `localhost:8081`;
 * Run `make` to package the jar;
 * Upload the jar manually and submit the job.

Grafana is available at port 3000.

For producing data to Kafka, follow [these](sources/README.md) instructions.
