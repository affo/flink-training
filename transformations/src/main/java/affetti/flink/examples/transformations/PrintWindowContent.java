package affetti.flink.examples.transformations;

import affetti.flink.examples.common.RandomIntegersSourceWDelay;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 22/11/17.
 */
public class PrintWindowContent {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // can increase

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Integer> integers = env
                .addSource(new RandomIntegersSourceWDelay(10, 100, 5));

        integers.addSink(new SinkFunction<Integer>() {
            @Override
            public void invoke(Integer value, Context context) throws Exception {
                System.out.println(">>> (ts, el): " + context.timestamp() + ", " + value);
            }
        });

        integers
                .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(3)))
                .apply(
                        new AllWindowFunction<Integer, String, TimeWindow>() {
                            @Override
                            public void apply(
                                    TimeWindow timeWindow,
                                    Iterable<Integer> content,
                                    Collector<String> collector) throws Exception {
                                StringBuilder repr = new StringBuilder(timeWindow.getEnd() + " - ");
                                for (Integer element : content) {
                                    repr.append(element.toString()).append(" ");
                                }

                                collector.collect(repr.toString());
                            }
                        })
                .print();

        env.execute();
    }
}
