package my.flink.examples.transformations;

import my.flink.examples.common.FirstFieldTimestampExtractor;
import my.flink.examples.common.SensorsInRoomsSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 22/11/17.
 */
public class Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // can increase

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<Integer, String, Double>> temperatures = env
                .addSource(new SensorsInRoomsSource(100, 35d))
                .assignTimestampsAndWatermarks(new FirstFieldTimestampExtractor<>());

        temperatures
                .keyBy(1)
                .process(new LastAccessedFunction())
                .print();

        env.execute();
    }

    private static class LastAccessedState {
        public String key;
        public long lastAccessed;
    }

    private static class LastAccessedFunction extends
            ProcessFunction<Tuple3<Integer, String, Double>, Tuple2<Integer, String>> {
        private ValueState<LastAccessedState> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("state", LastAccessedState.class));
        }

        @Override
        public void processElement(
                Tuple3<Integer, String, Double> measurement,
                Context context,
                Collector<Tuple2<Integer, String>> collector) throws Exception {

            LastAccessedState current = state.value();
            if (current == null) {
                current = new LastAccessedState();
                current.key = measurement.f1;
            }

            current.lastAccessed = context.timestamp();
            state.update(current);

            context.timerService().registerEventTimeTimer(current.lastAccessed + 5L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                            Collector<Tuple2<Integer, String>> out) throws Exception {
            LastAccessedState result = state.value();

            if (timestamp == result.lastAccessed + 5L) {
                out.collect(Tuple2.of((int) timestamp, result.key));
            }
        }
    }

}
