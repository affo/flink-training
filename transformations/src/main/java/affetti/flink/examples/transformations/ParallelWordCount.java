package affetti.flink.examples.transformations;

import affetti.flink.examples.common.RandomWordsSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by affo on 22/11/17.
 */
public class ParallelWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> words = env
                .addSource(new RandomWordsSource(10))
                .returns(String.class);

        words
                .keyBy(w -> w)
                .map(new BasicWordCount.WordCounter())
                .print();

        env.execute();
    }
}
