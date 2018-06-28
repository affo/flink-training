package affetti.flink.examples.transformations;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by affo on 24/11/17.
 */
public class Aligner<T1, T2> implements CoFlatMapFunction<T1, T2, Tuple2<T1, T2>> {
    private List<T1> one = new LinkedList<>();
    private List<T2> two = new LinkedList<>();

    private void collectIf(Collector<Tuple2<T1, T2>> collector) {
        if (!one.isEmpty() && !two.isEmpty()) {
            T1 first = one.remove(0);
            T2 second = two.remove(0);
            collector.collect(Tuple2.of(first, second));
        }
    }

    @Override
    public void flatMap1(T1 element,
                         Collector<Tuple2<T1, T2>> collector) throws Exception {
        one.add(element);
        collectIf(collector);
    }

    @Override
    public void flatMap2(T2 element,
                         Collector<Tuple2<T1, T2>> collector) throws Exception {
        two.add(element);
        collectIf(collector);
    }
}
