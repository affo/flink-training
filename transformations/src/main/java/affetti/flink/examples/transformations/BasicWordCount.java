package affetti.flink.examples.transformations;

import affetti.flink.examples.common.RandomWordsSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 22/11/17.
 */
public class BasicWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // try to increase and explain results

        DataStream<String> words = env
                .addSource(new RandomWordsSource(10))
                .returns(String.class);

        words
                .map(new WordCounter())
                .print();

        env.execute();
    }

    public static class WordCounter implements MapFunction<String, Tuple2<String, Integer>> {
        private Map<String, Integer> counts = new HashMap<>();

        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            int count = counts.getOrDefault(s, 0);
            counts.put(s, ++count);
            return Tuple2.of(s, count);
        }
    }
}
