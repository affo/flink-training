package my.flink.examples.transformations;

import my.flink.examples.common.RandomIntegersSourceWDelay;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 22/11/17.
 */
public class PrintWindowContent {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // can increase

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<Integer, Integer>> integers = env
                .addSource(new RandomIntegersSourceWDelay(10, 100, 5))
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Tuple2<Integer, Integer>>(
                                Time.of(5, TimeUnit.MILLISECONDS)) {
                            @Override
                            public long extractTimestamp(Tuple2<Integer, Integer> t) {
                                return t.f0;
                            }
                        });

        integers.print();

        integers
                .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(3)))
                .apply(
                        new AllWindowFunction<Tuple2<Integer,Integer>, String, TimeWindow>() {
                            @Override
                            public void apply(
                                    TimeWindow timeWindow,
                                    Iterable<Tuple2<Integer, Integer>> content,
                                    Collector<String> collector) throws Exception {
                                String repr = timeWindow.getEnd() + " - ";
                                for (Tuple2<Integer, Integer> element : content) {
                                    repr += element.toString() + " ";
                                }

                                collector.collect(repr);
                            }
                        })
                .print();

        env.execute();
    }
}
