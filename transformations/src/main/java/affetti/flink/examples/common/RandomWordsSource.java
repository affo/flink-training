package affetti.flink.examples.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Created by affo on 22/11/17.
 */
public class RandomWordsSource implements SourceFunction<String> {
    public final static String[] DEFAULT_WORDS = {"foo", "bar", "buz"};
    private String[] words;
    private NumberGenerator generator;
    private boolean stop;
    private int limit;

    public RandomWordsSource(int limit) {
        this(limit, DEFAULT_WORDS);
    }

    public RandomWordsSource(int limit, String... words) {
        this.limit = limit;
        this.words = words;
        this.generator = new NumberGenerator(words.length);
        this.stop = false;
    }

    @Override
    public void run(SourceContext<String> context) throws Exception {
        while (limit > 0 && !stop) {
            Tuple2<Long, Integer> next = generator.nextInt();
            String nextWord = words[next.f1];
            context.collectWithTimestamp(nextWord, next.f0);
            context.emitWatermark(new Watermark(next.f0 - 1));
            limit--;
        }
    }

    @Override
    public void cancel() {
        this.stop = true;
    }
}
