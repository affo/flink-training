package affetti.flink.examples.fault_tolerance;

import affetti.flink.examples.transformations.ParallelWordCountV2;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 22/11/17.
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        // checkpointing configuration
        env.enableCheckpointing(10000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                60, Time.of(10, TimeUnit.SECONDS)));
        StateBackend sb = new FsStateBackend("file:///tmp/checkpoints");
        env.setStateBackend(sb);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("kafka-host", "localhost") + ":9092");
        properties.setProperty("group.id", "test");

        DataStream<String> words = env.addSource(
                new FlinkKafkaConsumer010<>(
                        "test", new SimpleStringSchema(), properties
                ))
                .name("FromKafka");

        DataStream<Tuple2<String, Integer>> out = words
                .keyBy(w -> w)
                .map(new ParallelWordCountV2.WordCounter())
                .name("WordCounter");

        out.addSink(new InfluxDBSink<>())
                .name("ToInfluxDB").setParallelism(1);

        env.execute();
    }
}
