package affetti.flink.training;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 21/09/17.
 */
public class HourlyTips {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TaxiFare> fares = env.addSource(
                new TaxiFareSource("nycTaxiFares.gz", 60, 2000)
        );


        /*
        // ----------- Solution 1
        DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
                .keyBy("driverId")
                .timeWindow(Time.hours(1))
                .sum("tip")
                .timeWindowAll(Time.hours(1))
                .maxBy("tip")
                .map(fare -> Tuple3.of(-1L, fare.driverId, fare.tip)) // how to add the window timestamp?
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(Long.class, Long.class, Float.class));

        hourlyTips.print();
        */

        /*
        // ----------- Solution 2
        DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
                .keyBy((TaxiFare fare) -> fare.driverId)
                .timeWindow(Time.hours(1))
                .apply(new SlowSumTips());

        // find the highest total tips in each hour
        DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
                .timeWindowAll(Time.hours(1))
                .maxBy(2);

        hourlyMax.print();
        */

        // ----------- Solution 3
        DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
                .keyBy("driverId")
                .timeWindow(Time.hours(1))
                .aggregate(new SumTips(), new AddTimeStamp());

        hourlyTips
                .timeWindowAll(Time.hours(1))
                .maxBy(2)
                .print();

        env.execute();
    }

    private static class SumTips implements AggregateFunction<TaxiFare, Tuple2<Long, Float>, Tuple2<Long, Float>> {
        @Override
        public Tuple2<Long, Float> createAccumulator() {
            return Tuple2.of(0L, 0f);
        }

        @Override
        public Tuple2<Long, Float> add(TaxiFare taxiFare, Tuple2<Long, Float> acc) {
            if (acc.f0 == 0) {
                acc.f0 = taxiFare.driverId;
            }
            acc.f1 += taxiFare.tip;
            return acc;
        }

        @Override
        public Tuple2<Long, Float> getResult(Tuple2<Long, Float> acc) {
            return acc;
        }

        @Override
        public Tuple2<Long, Float> merge(Tuple2<Long, Float> acc1, Tuple2<Long, Float> acc2) {
            acc1.f1 += acc2.f1;
            return acc1;
        }
    }

    private static class AddTimeStamp extends ProcessWindowFunction<Tuple2<Long, Float>, Tuple3<Long, Long, Float>, Tuple, TimeWindow> {
        @Override
        public void process(
                Tuple key,
                Context context,
                Iterable<Tuple2<Long, Float>> iterable,
                Collector<Tuple3<Long, Long, Float>> collector) throws Exception {

            Tuple2<Long, Float> result = iterable.iterator().next();
            collector.collect(Tuple3.of(context.window().getEnd(), result.f0, result.f1));
        }
    }


    public static class SlowSumTips implements WindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
        @Override
        public void apply(
                Long key,
                TimeWindow window,
                Iterable<TaxiFare> fares,
                Collector<Tuple3<Long, Long, Float>> out) throws Exception {

            float sumOfTips = 0;
            for (TaxiFare fare : fares) {
                sumOfTips += fare.tip;
            }

            out.collect(new Tuple3<>(window.getEnd(), key, sumOfTips));
        }
    }
}
