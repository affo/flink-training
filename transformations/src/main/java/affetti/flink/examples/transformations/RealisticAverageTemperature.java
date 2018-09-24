package affetti.flink.examples.transformations;

import affetti.flink.examples.common.RealisticSensorsInRoomsSource;
import affetti.flink.examples.common.SocketController;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 22/11/17.
 */
public class RealisticAverageTemperature {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String, Double>> temperatures = env
                .addSource(
                        new RealisticSensorsInRoomsSource(
                                20, 35d, 10,
                                true, index -> new SocketController("localhost", 8000 + index)));

        temperatures
                .keyBy(0) // keyBy room name
                .timeWindow(Time.milliseconds(5))
                .aggregate(new SensorUtils.Averager(), new SensorUtils.Averager.ForMeta())
                .print();

        env.execute();
    }


}
