package affetti.flink.examples.transformations;

import affetti.flink.examples.common.Printer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by affo on 22/11/17.
 */
public class SplitSelect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //DataStreamSource<Integer> integers = env.addSource(new RandomIntegersSource(1000));
        DataStreamSource<Integer> integers = env.fromElements(30, 10, 9);

        integers.addSink(new Printer<>("Integers"));

        SplitStream<Integer> split = integers
                .split((i) -> {
                    List<String> tags = new ArrayList<>();

                    if (i % 2 == 0) {
                        tags.add("div2");
                    }

                    if (i % 3 == 0) {
                        tags.add("div3");
                    }

                    if (i % 5 == 0) {
                        tags.add("div5");
                    }

                    return tags;
                });

        // receives the elements that match one or more tags
        DataStream<Integer> div3AndOr5 = split.select("div3", "div5");
        div3AndOr5.addSink(new Printer<>("Divisible by 3 and/or 5"));
        DataStream<Integer> div2 = split.select("div2");
        div2.addSink(new Printer<>("Divisible by 2"));

        div3AndOr5
                .map(i -> i * i)
                .addSink(new Printer<>("Square"));

        div2
                .map(i -> i * i * i)
                .addSink(new Printer<>("Cube"));

        env.execute();
    }
}
