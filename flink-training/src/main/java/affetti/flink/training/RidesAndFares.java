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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 21/09/17.
 */
public class RidesAndFares {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource("nycTaxiRides.gz", 60, 600)
        )
                .filter(ride -> ride.isStart)
                .keyBy("rideId");

        DataStream<TaxiFare> fares = env.addSource(
                new TaxiFareSource("nycTaxiFares.gz", 60, 600)
        )
                .keyBy("rideId");

        DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides.connect(fares)
                .flatMap(new EnrichRidesWithFares());

        enrichedRides.print();

        env.execute();
    }

    private static class EnrichRidesWithFares extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
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
        public void flatMap1(TaxiRide taxiRide, Collector<Tuple2<TaxiRide, TaxiFare>> collector) throws Exception {
            TaxiFare fare = fares.value();
            if (fare != null) {
                fares.clear();
                collector.collect(Tuple2.of(taxiRide, fare));
            } else {
                rides.update(taxiRide);
            }
        }

        @Override
        public void flatMap2(TaxiFare taxiFare, Collector<Tuple2<TaxiRide, TaxiFare>> collector) throws Exception {
            TaxiRide ride = rides.value();
            if (ride != null) {
                rides.clear();
                collector.collect(Tuple2.of(ride, taxiFare));
            } else {
                fares.update(taxiFare);
            }
        }
    }
}
