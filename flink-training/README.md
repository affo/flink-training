## Flink Official Training

We focus on the official Flink training for streaming available at http://training.data-artisans.com/.

Follow the setup at http://training.data-artisans.com/devEnvSetup.html.

### Super TL;DR

```
$ ./install.sh
```

### TL;DR (What `install.sh` does)

Clone the `flink-training-exercises` repository in the main folder (next to `flink-training`):

```
git clone https://github.com/dataArtisans/flink-training-exercises.git
cd flink-training-exercises
mvn clean install -DskipTests -Drat.ignoreErrors=true
```

And refresh your maven project (the POM depends on that module).  
Download the data set and place it in the folder `flink-training`:

```
wget http://training.data-artisans.com/trainingData/nycTaxiRides.gz
wget http://training.data-artisans.com/trainingData/nycTaxiFares.gz
```
