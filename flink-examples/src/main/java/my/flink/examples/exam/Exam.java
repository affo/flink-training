package my.flink.examples.exam;

import my.flink.examples.common.FirstFieldTimestampExtractor;
import my.flink.examples.common.SensorsInRoomsSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by affo on 22/11/17.
 */
public class Exam {
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

        env.execute();
    }
}
