package affetti.flink.training;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 21/09/17.
 */
public class LongRideAlerts {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", "nycTaxiRides.gz");

        DataStream<TaxiRide> rides = env.addSource(
                new CheckpointedTaxiRideSource(input, 6000)
        );

        rides
                .keyBy(ride -> ride.rideId)
                .process(new LongRides()).print();

        env.execute();
    }

    private static class LongRides extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {
        private transient ValueState<TaxiRide> rides;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<TaxiRide> descriptor = new ValueStateDescriptor<>("rides", TaxiRide.class);
            rides = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(TaxiRide taxiRide, Context context, Collector<TaxiRide> collector) throws Exception {
            if (taxiRide.isStart) {
                // if END happened before, don't overwrite it
                if (rides.value() == null) {
                    rides.update(taxiRide);
                }
            } else {
                rides.update(taxiRide);
            }

            context.timerService().registerEventTimeTimer(taxiRide.getEventTime() + TimeUnit.HOURS.toMillis(2));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<TaxiRide> out) throws Exception {
            TaxiRide ride = rides.value();
            if (ride != null && ride.isStart) {
                out.collect(ride);
            }
            rides.clear();
        }
    }
}
