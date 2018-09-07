package affetti.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Created by affo on 07/09/18.
 *
 * Courtesy of
 * https://kafka.apache.org/20/documentation/streams/tutorial
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("Provide lines topic, please.");
        }

        String wordsTopic = args[0];
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream(wordsTopic);
        source
                // split values
                .flatMapValues(
                        value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"))
                )
                // shuffle data using the words as keys
                .groupBy((key, value) -> value)
                // The count operator has a Materialized parameter that specifies that the running count
                // should be stored in a state store named counts-store that can be queried in real-time.
                .count(Materialized.as("counts-store"))
                // The aggregation produces a KTable!
                .toStream()
                // just to use the standard console consumer to output records
                // with default deserializers:
                .mapValues(count -> Long.toString(count))
                .to("wordcount-output", Produced.with(Serdes.String(), Serdes.String()));

        /*
        In order to print the key-value pair with the console consumer use:
        $ kafka-console-consumer.sh --bootstrap-server localhost:9092 \
            --topic wordcount-output --from-beginning --property print.key=true
        */

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
