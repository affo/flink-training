package my.flink.examples.transformations;

import my.flink.examples.common.FirstFieldTimestampExtractor;
import my.flink.examples.common.SensorsInRoomsSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 22/11/17.
 */
public class AverageTemperature {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // can increase

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        TypeHint<Tuple4<Integer, String, Double, Integer>> outType =
                new TypeHint<Tuple4<Integer, String, Double, Integer>>() {
                };

        DataStream<Tuple3<Integer, String, Double>> temperatures = env
                .addSource(new SensorsInRoomsSource(20, 35d))
                .assignTimestampsAndWatermarks(new FirstFieldTimestampExtractor<>());

        temperatures
                .map(t3 -> Tuple4.of(t3.f0, t3.f1, t3.f2, 1))
                .returns(outType)
                .keyBy(1) // keyBy room name
                .window(SlidingEventTimeWindows.of(Time.milliseconds(15), Time.milliseconds(5)))
                .reduce(new SensorUtils.Summer()) // , new SensorUtils.OnlyForMeta()) // -> use this to have same timestamps without losing performance
                .map(t -> {
                    t.f2 = t.f2 / t.f3;
                    return t;
                })
                .returns(outType)
                .print();

        env.execute();

        System.out.println("\n>>> Alternatively:\n");
        // explain the difference in behaviour for the windowIds

        temperatures
                .map(t3 -> Tuple4.of(t3.f0, t3.f1, t3.f2, 1))
                .returns(outType)
                .keyBy(1) // keyBy room name
                .window(SlidingEventTimeWindows.of(Time.milliseconds(15), Time.milliseconds(5)))
                .apply(new WindowAverager())
                .print();

        env.execute();
    }

    private static class WindowAverager implements WindowFunction<
            Tuple4<Integer, String, Double, Integer>,
            Tuple4<Integer, String, Double, Integer>,
            Tuple, TimeWindow> {

        @Override
        public void apply(Tuple key, TimeWindow window,
                          Iterable<Tuple4<Integer, String, Double, Integer>> input,
                          Collector<Tuple4<Integer, String, Double, Integer>> out) {
            int count = 0;
            double sum = 0;

            for (Tuple4<Integer, String, Double, Integer> element : input) {
                count++;
                sum += element.f2;
            }

            String roomName = key.getField(0).toString();
            out.collect(Tuple4.of((int) window.getEnd(), roomName, sum / count, count));
        }
    }
}
