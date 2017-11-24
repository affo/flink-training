package my.flink.examples.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Created by affo on 22/11/17.
 */
public class RandomWordsSource implements SourceFunction<Tuple2<Integer, String>> {
    public final static String[] DEFAULT_WORDS = {"foo", "bar", "buz"};
    private String[] words;
    private IntegersGenerator generator;
    private boolean stop;
    private int limit;

    public RandomWordsSource(int limit) {
        this(limit, DEFAULT_WORDS);
    }

    public RandomWordsSource(int limit, String... words) {
        this.limit = limit;
        this.words = words;
        this.generator = new IntegersGenerator(words.length);
        this.stop = false;
    }

    @Override
    public void run(SourceContext<Tuple2<Integer, String>> context) throws Exception {
        while (limit > 0 && !stop) {
            Tuple2<Integer, Integer> next = generator.next();
            String nextWord = words[next.f1];
            context.collect(Tuple2.of(next.f0, nextWord));
            limit--;
        }
    }

    @Override
    public void cancel() {
        this.stop = true;
    }
}
