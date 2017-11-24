package my.flink.examples.common;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Created by affo on 22/11/17.
 */
public class RandomIntegersSource implements SourceFunction<Integer> {
    public static final int MAX_VALUE = 10;
    private IntegersGenerator generator;
    private boolean stop;
    private int limit;

    public RandomIntegersSource(int limit) {
        this(limit, MAX_VALUE);
    }

    public RandomIntegersSource(int limit, int maxValue) {
        this.limit = limit;
        this.generator = new IntegersGenerator(maxValue);
        this.stop = false;
    }

    @Override
    public void run(SourceContext<Integer> context) throws Exception {
        while (limit > 0 && !stop) {
            int next = generator.next().f1;
            context.collect(next);
            limit--;
        }
    }

    @Override
    public void cancel() {
        this.stop = true;
    }
}
