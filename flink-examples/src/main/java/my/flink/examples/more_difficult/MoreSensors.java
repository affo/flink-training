package my.flink.examples.more_difficult;

import my.flink.examples.common.FirstFieldTimestampExtractor;
import my.flink.examples.common.SensorsInRoomsSource;
import my.flink.examples.transformations.Aligner;
import my.flink.examples.transformations.SensorUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by affo on 22/11/17.
 */
public class MoreSensors {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<Integer, String, Double>> temperatures = env
                .addSource(new SensorsInRoomsSource(100, 35d)) // in degrees
                .assignTimestampsAndWatermarks(new FirstFieldTimestampExtractor<>());

        DataStream<Tuple3<Integer, String, Double>> pressures = env
                .addSource(new SensorsInRoomsSource(100, 100d)) // in atm
                .assignTimestampsAndWatermarks(new FirstFieldTimestampExtractor<>());

        TypeInformation<Tuple4<Integer, String, Double, Integer>> typeInfo =
                TypeInformation.of(
                        new TypeHint<Tuple4<Integer, String, Double, Integer>>() {
                        }
                );

        TupleTypeInfo<
                Tuple2<
                        Tuple4<Integer, String, Double, Integer>,
                        Tuple4<Integer, String, Double, Integer>
                        >
                > finalType = new TupleTypeInfo<>(typeInfo, typeInfo);

        // TODO your code goes here

        DataStream<Tuple4<Integer, String, Double, Integer>> averagedPressures = pressures
                .map(t3 -> Tuple4.of(t3.f0, t3.f1, t3.f2, 1))
                .returns(typeInfo)
                .keyBy(1)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(3), Time.milliseconds(1)))
                .reduce(new SensorUtils.Summer(), new SensorUtils.OnlyForMeta())
                .map(t -> {
                    t.f2 = t.f2 / t.f3;
                    return t;
                })
                .returns(typeInfo);


        DataStream<Tuple4<Integer, String, Double, Integer>> maxTemperatures = temperatures
                .map(t3 -> Tuple4.of(t3.f0, t3.f1, t3.f2, 1))
                .returns(typeInfo)
                .keyBy(1)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(5), Time.milliseconds(1)))
                .reduce(
                        new ReduceFunction<Tuple4<Integer, String, Double, Integer>>() {
                            @Override
                            public Tuple4<Integer, String, Double, Integer> reduce(
                                    Tuple4<Integer, String, Double, Integer> accumulator,
                                    Tuple4<Integer, String, Double, Integer> next) throws Exception {
                                double max = Math.max(accumulator.f2, next.f2);
                                return Tuple4.of(next.f0, next.f1, max, accumulator.f3 + 1);
                            }
                        }, new SensorUtils.OnlyForMeta());


        averagedPressures
                .connect(maxTemperatures)
                .flatMap(new Aligner<>())
                .returns(finalType)
                .print();

        env.execute();
    }
}
