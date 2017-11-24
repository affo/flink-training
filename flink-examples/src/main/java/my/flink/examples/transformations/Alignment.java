package my.flink.examples.transformations;

import my.flink.examples.common.Printer;
import my.flink.examples.common.RandomIntegersSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by affo on 22/11/17.
 */
public class Alignment {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> s1 = env.addSource(new RandomIntegersSource(10));
        DataStreamSource<Integer> s2 = env.addSource(new RandomIntegersSource(10));

        s1.addSink(new Printer<>("S1"));
        s2.addSink(new Printer<>("S2"));

        s1.connect(s2)
                .flatMap(new Aligner<>())
                .print();

        env.execute();
    }
}
