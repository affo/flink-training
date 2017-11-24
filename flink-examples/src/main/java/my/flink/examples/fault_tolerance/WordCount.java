package my.flink.examples.fault_tolerance;

import my.flink.examples.transformations.ParallelWordCountV2;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 22/11/17.
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                60, Time.of(10, TimeUnit.SECONDS)));
        env.setStateBackend(new FsStateBackend("file:///tmp/checkpoints"));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", K.KAFKA_HOST + ":9092");
        properties.setProperty("group.id", "test");

        DataStream<String> words = env.addSource(
                new FlinkKafkaConsumer010<>(
                        "test", new SimpleStringSchema(), properties
                ));

        DataStream<Tuple2<String, Integer>> out = words
                .keyBy(w -> w)
                .map(new ParallelWordCountV2.WordCounter());

        out.addSink(new InfluxDBSink<>()).setParallelism(1);
        out.print();

        env.execute();
    }
}
