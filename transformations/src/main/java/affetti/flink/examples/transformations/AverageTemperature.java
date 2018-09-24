package affetti.flink.examples.transformations;

import affetti.flink.examples.common.SensorsInRoomsSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
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

        TupleTypeInfo<Tuple3<String, Double, Integer>> t3Type =
                TupleTypeInfo.getBasicTupleTypeInfo(String.class, Double.class, Integer.class);
        TupleTypeInfo<Tuple4<Long, String, Double, Integer>> t4Type =
                TupleTypeInfo.getBasicTupleTypeInfo(Long.class, String.class, Double.class, Integer.class);

        DataStream<Tuple2<String, Double>> temperatures = env
                .addSource(new SensorsInRoomsSource(20, 35d));

        temperatures
                .keyBy(0) // keyBy room name
                .window(SlidingEventTimeWindows.of(Time.milliseconds(15), Time.milliseconds(5)))
                .process(new WindowAverager())
                .print();

        env.execute();

        System.out.println("\n>>> Alternatively, with aggregate function:\n");
        // gain in performance wrt ProcessWindowFunction
        temperatures
                .keyBy(0) // keyBy room name
                .timeWindow(Time.milliseconds(15), Time.milliseconds(5))
                .aggregate(new SensorUtils.Averager(), new SensorUtils.Averager.ForMeta()) // -> use the function to enrich output with metadata about windowing
                .print();

        env.execute();

        System.out.println("\n>>> Alternatively, with reduce function:\n");
        // explain the difference in behaviour for the windowIds

        temperatures
                .map(t2 -> Tuple3.of(t2.f0, t2.f1, 1))
                .returns(t3Type)
                .keyBy(0) // keyBy room name
                .window(SlidingEventTimeWindows.of(Time.milliseconds(15), Time.milliseconds(5)))
                .reduce(new SensorUtils.Summer(), new SensorUtils.Summer.ForMeta())
                .map(t -> {
                    t.f2 = t.f2 / t.f3;
                    return t;
                })
                .returns(t4Type)
                .print();

        env.execute();
    }

    private static class WindowAverager extends ProcessWindowFunction<
            Tuple2<String, Double>,
            Tuple3<Long, String, Double>, Tuple, TimeWindow> {

        @Override
        public void process(Tuple key, Context context, Iterable<Tuple2<String, Double>> input,
                            Collector<Tuple3<Long, String, Double>> out) throws Exception {
            int count = 0;
            double sum = 0;

            for (Tuple2<String, Double> element : input) {
                count++;
                sum += element.f1;
            }

            String roomName = key.getField(0).toString();
            out.collect(Tuple3.of(context.window().getEnd(), roomName, sum / count));
        }
    }
}
