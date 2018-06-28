package affetti.flink.examples.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Created by affo on 22/11/17.
 */
public class RandomIntegersSource implements SourceFunction<Integer> {
    public static final int MAX_VALUE = 10;
    private NumberGenerator generator;
    private boolean stop;
    private int limit;

    public RandomIntegersSource(int limit) {
        this(limit, MAX_VALUE);
    }

    public RandomIntegersSource(int limit, int maxValue) {
        this.limit = limit;
        this.generator = new NumberGenerator(maxValue);
        this.stop = false;
    }

    @Override
    public void run(SourceContext<Integer> context) throws Exception {
        while (limit > 0 && !stop) {
            Tuple2<Long, Integer> next = generator.nextInt();
            context.collectWithTimestamp(next.f1, next.f0);
            context.emitWatermark(new Watermark(next.f0 - 1));
            limit--;
        }
    }

    @Override
    public void cancel() {
        this.stop = true;
    }
}
