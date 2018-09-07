package affetti.kafka.prodcons;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.net.Socket;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by affo on 07/09/18.
 */
public class SocketProducer {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("Provide destination topic, please.");
        }
        String topic = args[0];

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", Serdes.String().serializer().getClass());
        props.put("value.serializer", Serdes.String().serializer().getClass());

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            Socket socket = new Socket("localhost", 9999);
            Scanner scanner = new Scanner(socket.getInputStream());

            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String key = line.isEmpty() ? "" : line.substring(0, 1);
                producer.send(new ProducerRecord<>(topic, key, line));
            }
        } finally {
            producer.close();
        }
    }
}
