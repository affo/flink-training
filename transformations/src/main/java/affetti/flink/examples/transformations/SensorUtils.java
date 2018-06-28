package affetti.flink.examples.transformations;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 24/11/17.
 */
public class SensorUtils {
    public static class Summer implements ReduceFunction<Tuple3<String, Double, Integer>> {
        @Override
        public Tuple3<String, Double, Integer> reduce(
                Tuple3<String, Double, Integer> accumulator,
                Tuple3<String, Double, Integer> value) throws Exception {

            Double accumulatedSum = accumulator.f1 + value.f1;
            int accumulatedCount = accumulator.f2 + value.f2;
            return Tuple3.of(value.f0, accumulatedSum, accumulatedCount);
        }
    }

    public static class OnlyForMeta extends ProcessWindowFunction<
            Tuple3<String, Double, Integer>,
            Tuple4<Long, String, Double, Integer>,
            Tuple, TimeWindow> {

        @Override
        public void process(Tuple key, Context context,
                            Iterable<Tuple3<String, Double, Integer>> reduceResult,
                            Collector<Tuple4<Long, String, Double, Integer>> out) {
            Tuple3<String, Double, Integer> result = reduceResult.iterator().next();
            out.collect(Tuple4.of(context.window().getEnd(), result.f0, result.f1, result.f2));
        }
    }
}
