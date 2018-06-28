package affetti.flink.examples.transformations;

import affetti.flink.examples.common.SensorsInRoomsSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 22/11/17.
 */
public class Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // can increase

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String, Double>> temperatures = env
                .addSource(new SensorsInRoomsSource(100, 35d));

        temperatures
                .keyBy(0)
                .process(new LastAccessedFunction())
                .print();

        env.execute();
    }


    private static class LastAccessedFunction extends
            KeyedProcessFunction<Tuple, Tuple2<String, Double>, Tuple2<Long, String>> {
        private ValueState<Long> lastAccessed;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastAccessed = getRuntimeContext().getState(new ValueStateDescriptor<>("lastAccessed", Long.class));
        }

        @Override
        public void processElement(
                Tuple2<String, Double> measurement,
                Context context,
                Collector<Tuple2<Long, String>> collector) throws Exception {
            Long current = context.timestamp();
            lastAccessed.update(current);

            context.timerService().registerEventTimeTimer(current + 5L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                            Collector<Tuple2<Long, String>> out) throws Exception {
            if (timestamp == lastAccessed.value() + 5L) {
                out.collect(Tuple2.of(timestamp, ctx.getCurrentKey().toString()));
            }
        }
    }

}
