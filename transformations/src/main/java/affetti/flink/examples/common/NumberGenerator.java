package affetti.flink.examples.common;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by affo on 22/11/17.
 * <p>
 * Generates random numbers as (unique_incremental_id, number).
 */
public class NumberGenerator implements Serializable {
    protected Random random;
    protected final double range;
    private long count;

    /**
     *
     * @param range maxValue (excluded)
     */
    public NumberGenerator(double range) {
        this.range = range;
        this.random = new Random(0);
        this.count = 0;
    }

    public Tuple2<Long, Double> next() {
        double next = Math.abs(random.nextDouble()) * (range - 1);
        return Tuple2.of(count++, next);
    }

    public Tuple2<Long, Integer> nextInt() {
        Tuple2<Long, Double> next = next();
        return Tuple2.of(next.f0, (int) Math.round(next.f1));
    }

    public <T> Tuple2<Long, T> next(T element) {
        return Tuple2.of(count++, element);
    }

    public void clear() {
        this.count = 0;
    }
}
