package affetti.kafka.prodcons;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 07/09/18.
 *
 * Courtesy of
 * https://www.confluent.io/blog/tutorial-getting-started-with-the-new-apache-kafka-0-9-consumer-client/
 */
public class ConsoleConsumer {
    public static void main(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("Provide input topic, please.");
        }
        String topic = args[0];

        int numConsumers = 3;
        String groupId = "test-consumer-group";
        List<String> topics = Collections.singletonList(topic);
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        final List<ConsumerLoop> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            ConsumerLoop consumer = new ConsumerLoop(i, groupId, topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (ConsumerLoop consumer : consumers) {
                consumer.shutdown();
            }
            executor.shutdown();

            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }
}
