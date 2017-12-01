package my.flink.examples.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by affo on 22/11/17.
 */
public class RandomIntegersSourceWDelay implements SourceFunction<Tuple2<Integer, Integer>> {
    public static final int MAX_VALUE = 10;
    public static final int MAX_DELAY = 5;
    private IntegersGenerator generator;
    private boolean stop;
    private int limit, maxDelay;

    public RandomIntegersSourceWDelay(int limit) {
        this(limit, MAX_VALUE, MAX_DELAY);
    }

    public RandomIntegersSourceWDelay(int limit, int maxValue, int maxDelay) {
        this.limit = limit;
        this.generator = new IntegersGenerator(maxValue);
        this.maxDelay = maxDelay;
        this.stop = false;
    }

    @Override
    public void run(SourceContext<Tuple2<Integer, Integer>> context) throws Exception {
        while (limit > 0 && !stop) {
            int batchSize = maxDelay - limit;
            if (batchSize <= 0) {
                batchSize = maxDelay;
            }

            List<Tuple2<Integer, Integer>> batch = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                batch.add(generator.next());
            }

            Collections.shuffle(batch);

            for (Tuple2<Integer, Integer> element : batch) {
                context.collect(element);
            }

            limit -= batchSize;
        }
    }

    @Override
    public void cancel() {
        this.stop = true;
    }
}
