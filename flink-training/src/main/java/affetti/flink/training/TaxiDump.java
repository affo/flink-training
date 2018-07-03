package affetti.flink.training;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by affo on 21/09/17.
 */
public class TaxiDump {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input", "nycTaxiRides.gz");

        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource(input, 600)
        );
        rides.print();

        env.execute();
    }
}
