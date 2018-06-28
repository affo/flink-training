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
                .map(t2 -> Tuple3.of(t2.f0, t2.f1, 1))
                .returns(t3Type)
                .keyBy(0) // keyBy room name
                .window(SlidingEventTimeWindows.of(Time.milliseconds(15), Time.milliseconds(5)))
                .process(new WindowAverager())
                .print();

        env.execute();

        System.out.println("\n>>> Alternatively:\n");
        // explain the difference in behaviour for the windowIds

        temperatures
                .map(t2 -> Tuple3.of(t2.f0, t2.f1, 1))
                .returns(t3Type)
                .keyBy(0) // keyBy room name
                .window(SlidingEventTimeWindows.of(Time.milliseconds(15), Time.milliseconds(5)))
                .reduce(new SensorUtils.Summer()) //, new SensorUtils.OnlyForMeta()) // -> use this to have same timestamps without losing performance
                .map(t -> {
                    t.f1 = t.f1 / t.f2;
                    return t;
                })
                .returns(t3Type)
                .print();

        env.execute();

        System.out.println("\n>>> Alternatively, to preserve window end:\n");
        // explain the difference in behaviour for the windowIds

        temperatures
                .map(t2 -> Tuple3.of(t2.f0, t2.f1, 1))
                .returns(t3Type)
                .keyBy(0) // keyBy room name
                .window(SlidingEventTimeWindows.of(Time.milliseconds(15), Time.milliseconds(5)))
                .reduce(new SensorUtils.Summer(), new SensorUtils.OnlyForMeta()) // -> use this to have same timestamps without losing performance
                .map(t -> {
                    t.f2 = t.f2 / t.f3;
                    return t;
                })
                .returns(t4Type)
                .print();

        env.execute();
    }

    private static class WindowAverager extends ProcessWindowFunction<
            Tuple3<String, Double, Integer>,
            Tuple4<Long, String, Double, Integer>, Tuple, TimeWindow> {

        @Override
        public void process(Tuple key, Context context, Iterable<Tuple3<String, Double, Integer>> input,
                            Collector<Tuple4<Long, String, Double, Integer>> out) throws Exception {
            int count = 0;
            double sum = 0;

            for (Tuple3<String, Double, Integer> element : input) {
                count++;
                sum += element.f1;
            }

            String roomName = key.getField(0).toString();
            out.collect(Tuple4.of(context.window().getEnd(), roomName, sum / count, count));
        }
    }
}
