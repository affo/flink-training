package affetti.flink.examples.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by affo on 22/11/17.
 */
public class RandomIntegersSourceWDelay implements SourceFunction<Integer> {
    public static final int MAX_VALUE = 10;
    public static final int MAX_DELAY = 5;
    private NumberGenerator generator;
    private boolean stop;
    private int limit, maxDelay;

    public RandomIntegersSourceWDelay(int limit) {
        this(limit, MAX_VALUE, MAX_DELAY);
    }

    public RandomIntegersSourceWDelay(int limit, int maxValue, int maxDelay) {
        this.limit = limit;
        this.generator = new NumberGenerator(maxValue);
        this.maxDelay = maxDelay;
        this.stop = false;
    }

    @Override
    public void run(SourceContext<Integer> context) throws Exception {
        while (limit > 0 && !stop) {
            int batchSize = maxDelay - limit;
            if (batchSize <= 0) {
                batchSize = maxDelay;
            }

            List<Tuple2<Long, Integer>> batch = new ArrayList<>(batchSize);
            long maxTS = -1;
            for (int i = 0; i < batchSize; i++) {
                Tuple2<Long, Integer> next = generator.nextInt();
                batch.add(next);
                maxTS = next.f0;
            }

            Collections.shuffle(batch);

            for (Tuple2<Long, Integer> element : batch) {
                context.collectWithTimestamp(element.f1, element.f0);
            }
            // we know that this batch is over, so we can emit the maxTimestamp reached for it
            context.emitWatermark(new Watermark(maxTS));

            limit -= batchSize;
        }
    }

    @Override
    public void cancel() {
        this.stop = true;
    }
}
