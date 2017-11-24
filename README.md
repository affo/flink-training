## Clustering and System Integration

__NOTE:__ You can simply run the examples from the IDE and use what Docker Compose provides and
ignore the Flink Cluster.

In order to run a job in a Flink cluster (together with a working installation of Kafka):

 * run `docker-compose up` in a separate terminal;
 * set the preferred main class in the `flink-examples/pom.xml` file (optional);
 * open Flink's web dashboard available at `docker-host:8081` (something like `192.168.99.100:8081` if you are on MacOS);
 * Run `make` to package the jar;
 * Upload the jar manually and submit the job (you can set the entrypoint if you want).

You can also run the examples from the IDE (carefully setup the constants in class `K`).

Grafana is available at port 3000.

For producing/consuming to/from Kafka consult the README in `sources` folder.
