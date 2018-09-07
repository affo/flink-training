package affetti.kafka.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.io.File;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by affo on 07/09/18.
 */
public class CredentialsProducer {
    public static final String CREDENTIALS_TOPIC = "credentials-topic";

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("Provide .csv file for credentials, please.");
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", Serdes.String().serializer().getClass());
        props.put("value.serializer", Serdes.String().serializer().getClass());

        // produce from file
        String csvPath = args[0];
        Scanner scanner = new Scanner(new File(csvPath));
        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                // we supposed the input file is well-formed
                String[] tokens = line.split(",");
                String username = tokens[0];
                String password = tokens[1];
                producer.send(new ProducerRecord<>(CREDENTIALS_TOPIC, username, password));
            }
        } finally {
            scanner.close();
            producer.close();
        }
    }
}
