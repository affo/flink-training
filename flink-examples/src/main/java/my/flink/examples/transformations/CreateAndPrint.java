package my.flink.examples.transformations;

import my.flink.examples.common.Printer;
import my.flink.examples.common.RandomIntegersSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by affo on 22/11/17.
 */
public class CreateAndPrint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> integers = env.addSource(new RandomIntegersSource(1000));
        integers.addSink(new Printer<>("Result"));

        env.execute();

        System.out.println("\n>>> Alternatively:\n");

        integers.print();

        env.execute();
    }
}
