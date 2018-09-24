package affetti.flink.examples.transformations;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
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

        public static class ForMeta extends ProcessWindowFunction<
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

    public static class Averager implements
            AggregateFunction<
                    Tuple2<String, Double>,
                    Tuple2<Double, Integer>,
                    Double> {

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return Tuple2.of(0d, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(Tuple2<String, Double> t, Tuple2<Double, Integer> acc) {
            return Tuple2.of(acc.f0 + t.f1, acc.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> acc) {
            return acc.f0 / acc.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a1, Tuple2<Double, Integer> a2) {
            return Tuple2.of(a1.f0 + a2.f0, a1.f1 + a2.f1);
        }

        public static class ForMeta extends ProcessWindowFunction<
                Double,
                Tuple3<Long, String, Double>,
                Tuple, TimeWindow> {

            @Override
            public void process(
                    Tuple key, Context context, Iterable<Double> aggregated,
                    Collector<Tuple3<Long, String, Double>> out) throws Exception {
                Double result = aggregated.iterator().next();
                out.collect(Tuple3.of(context.window().getEnd(), key.getField(0), result));
            }
        }
    }
}
