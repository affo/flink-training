package my.flink.examples.common;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by affo on 22/11/17.
 * <p>
 * Generates random elements as (unique_incremental_id, element).
 * The set of elements from which the generator chooses is represented only by its cardinality.
 * The element to return depending on the index selected in the range must be returned by implementing
 * the abstract method `fromIndexInRange`.
 *
 * @param <T> the type of the elements generated
 */
public abstract class AbstractGenerator<T> implements Serializable {
    protected Random random;
    protected final int range;
    private int count;

    public AbstractGenerator(int range) {
        this.range = range;
        this.random = new Random(0);
        this.count = 0;
    }

    public Tuple2<Integer, T> next() {
        int next = random.nextInt(range);
        return Tuple2.of(count++, fromIndexInRange(next));
    }

    public Tuple2<Integer, T> next(T next) {
        return Tuple2.of(count++, next);
    }

    public void clear() {
        this.count = 0;
    }

    /**
     *
     * @param index 0 <= index < range
     * @return
     */
    public abstract T fromIndexInRange(int index);
}
