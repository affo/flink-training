package affetti.flink.training;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 21/09/17.
 */
public class ExpireState {
    static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {
    };
    static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource("nycTaxiRides.gz", 60, 600)
        )
                .filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0)))
                .keyBy("rideId");

        DataStream<TaxiFare> fares = env.addSource(
                new TaxiFareSource("nycTaxiFares.gz", 60, 600)
        )
                .keyBy("rideId");

        SingleOutputStreamOperator<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides.connect(fares)
                .process(new EnrichRidesWithFares());

        enrichedRides.getSideOutput(unmatchedFares)
                .print();

        env.execute();
    }

    private static class EnrichRidesWithFares extends CoProcessFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
        // keyed, managed state
        private ValueState<TaxiRide> rides;
        private ValueState<TaxiFare> fares;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            rides = getRuntimeContext().getState(new ValueStateDescriptor<>("rides", TaxiRide.class));
            fares = getRuntimeContext().getState(new ValueStateDescriptor<>("fares", TaxiFare.class));
        }

        @Override
        public void processElement1(TaxiRide taxiRide, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> collector) throws Exception {
            TaxiFare fare = fares.value();
            if (fare != null) {
                fares.clear();
                collector.collect(Tuple2.of(taxiRide, fare));
            } else {
                rides.update(taxiRide);
                // as soon as the watermark arrives, we can stop waiting for the corresponding fare.
                // fares and rides happen at the same timestamp
                context.timerService().registerEventTimeTimer(taxiRide.getEventTime());
            }
        }

        @Override
        public void processElement2(TaxiFare taxiFare, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> collector) throws Exception {
            TaxiRide ride = rides.value();
            if (ride != null) {
                rides.clear();
                collector.collect(Tuple2.of(ride, taxiFare));
            } else {
                fares.update(taxiFare);
                // same as above
                context.timerService().registerEventTimeTimer(taxiFare.getEventTime());
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TaxiRide ride = rides.value();
            TaxiFare fare = fares.value();
            rides.clear();
            fares.clear();

            if (ride != null) {
                ctx.output(unmatchedRides, ride);
            }

            if (fare != null) {
                ctx.output(unmatchedFares, fare);
            }
        }
    }
}
