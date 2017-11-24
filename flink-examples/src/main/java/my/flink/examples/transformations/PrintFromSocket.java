package my.flink.examples.transformations;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by affo on 22/11/17.
 */
public class PrintFromSocket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> fromSocket = env.socketTextStream("localhost", 9999);
        fromSocket
                .map(String::toUpperCase)
                .map(s -> ">>> " + s)
                .print();

        env.execute();
    }
}
