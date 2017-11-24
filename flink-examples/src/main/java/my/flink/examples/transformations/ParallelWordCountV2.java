package my.flink.examples.transformations;

import my.flink.examples.common.RandomWordsSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by affo on 22/11/17.
 */
public class ParallelWordCountV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> words = env
                .addSource(new RandomWordsSource(10))
                .map(t -> t.f1)
                .returns(String.class);

        words
                .keyBy(w -> w)
                .map(new WordCounter())
                .print();

        env.execute();
    }

    public static class WordCounter extends RichMapFunction<String, Tuple2<String, Integer>> {
        private transient ValueState<Integer> count;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Integer> sd = new ValueStateDescriptor<>("counter", Integer.class);
            count = getRuntimeContext().getState(sd);
        }

        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            Integer count = this.count.value();
            if (count == null) {
                count = 0;
            }
            this.count.update(++count);
            return Tuple2.of(s, count);
        }
    }
}
