package affetti.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Created by affo on 07/09/18.
 *
 * Courtesy of
 * https://kafka.apache.org/20/documentation/streams/tutorial
 */
public class Login {
    public static final String OUTPUT_TOPIC = "accesses-topic";

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("Provide login requests topic, please.");
        }

        String loginTopic = args[0];
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-login");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> logins = builder.stream(loginTopic);
        logins = logins.filter((key, value) -> {
            // the key is meaningless, we parse the value
            boolean ok = value.matches("\\w+,\\w+");
            if (!ok) {
                System.err.println("Invalid login request: " + value);
            } else {
                System.out.println("Valid login request: " + value);
            }
            return ok;
        });

        KStream<String, String> kvLogins = logins.map((key, value) -> {
            String[] tokens = value.split(",");
            return new KeyValue<>(tokens[0], tokens[1]);
        });

        KTable<String, String> credentials = builder.table(CredentialsProducer.CREDENTIALS_TOPIC);

        KStream<String, Boolean> accesses = kvLogins.join(credentials,
                (providedPassword, correctPassword) -> providedPassword.equals(correctPassword));

        KStream<String, String> outputLog = accesses.map(
                (username, granted) -> {
                    String log = granted ? "Access granted to " + username :
                            "Access denied to " + username;
                    return new KeyValue<>(null, log);
                }
        );

        outputLog.to(OUTPUT_TOPIC);

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
