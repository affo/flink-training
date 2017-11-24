package my.flink.examples.transformations;

import my.flink.examples.common.RandomIntegersSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 22/11/17.
 */
public class FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> integers = env.addSource(new RandomIntegersSource(100));
        integers
                .flatMap(new FlatMapper())
                .print();

        env.execute();
    }

    private static class FlatMapper implements FlatMapFunction<Integer, Character> {
        private static final int offset = 96;
        // WARNING: stateful ==> possible side-effect
        private int count = 0;

        private Character nextChar() {
            count++;
            if (count > 26) {
                count = 1;
            }
            return (char) (count + offset);
        }

        @Override
        public void flatMap(Integer integer, Collector<Character> collector) throws Exception {
            char c = nextChar();
            for (int i = 0; i < integer; i++) {
                collector.collect(c);
            }
        }
    }
}
