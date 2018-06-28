package affetti.flink.examples.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Created by affo on 22/11/17.
 */
public class SensorsInRoomsSource implements SourceFunction<Tuple2<String, Double>> {
    public final static String[] ROOMS = {"r1", "r2", "r3", "r4"};
    private NumberGenerator generator;
    private NumberGenerator roomIndex;
    private boolean stop;
    private int limit;

    public SensorsInRoomsSource(int limit, double maxValueForRelevation) {
        this.limit = limit;
        this.generator = new NumberGenerator((int) Math.round(maxValueForRelevation));
        this.roomIndex = new NumberGenerator(ROOMS.length);
        this.stop = false;
    }

    @Override
    public void run(SourceContext<Tuple2<String, Double>> context) throws Exception {
        while (limit > 0 && !stop) {
            Tuple2<Long, Double> next = generator.next();
            String room = ROOMS[roomIndex.nextInt().f1];
            context.collectWithTimestamp(Tuple2.of(room, next.f1), next.f0);
            context.emitWatermark(new Watermark(next.f0 - 1));
            limit--;
        }
    }

    @Override
    public void cancel() {
        this.stop = true;
    }
}
