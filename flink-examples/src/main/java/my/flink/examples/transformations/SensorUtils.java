package my.flink.examples.transformations;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 24/11/17.
 */
public class SensorUtils {
    public static class Summer implements ReduceFunction<Tuple4<Integer, String, Double, Integer>> {
        @Override
        public Tuple4<Integer, String, Double, Integer> reduce(
                Tuple4<Integer, String, Double, Integer> accumulator,
                Tuple4<Integer, String, Double, Integer> value) throws Exception {

            Double accumulatedSum = accumulator.f2 + value.f2;
            int accumulatedCount = accumulator.f3 + value.f3;
            int windowEnd = Math.max(accumulator.f0, value.f0);
            return Tuple4.of(windowEnd, value.f1, accumulatedSum, accumulatedCount);
        }
    }

    public static class OnlyForMeta implements WindowFunction<
            Tuple4<Integer, String, Double, Integer>,
            Tuple4<Integer, String, Double, Integer>,
            Tuple, TimeWindow> {

        @Override
        public void apply(Tuple key, TimeWindow window,
                          Iterable<Tuple4<Integer, String, Double, Integer>> reduceResult,
                          Collector<Tuple4<Integer, String, Double, Integer>> out) {
            Tuple4<Integer, String, Double, Integer> result = reduceResult.iterator().next();
            out.collect(Tuple4.of((int) window.getEnd(), result.f1, result.f2, result.f3));
        }
    }
}
