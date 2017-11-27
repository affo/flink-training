package my.flink.examples.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Created by affo on 22/11/17.
 */
public class SensorsInRoomsSource implements SourceFunction<Tuple3<Integer, String, Double>> {
    public final static String[] ROOMS = {"r1", "r2", "r3", "r4"};
    private DoublesGenerator generator;
    private IntegersGenerator roomIndex;
    private boolean stop;
    private int limit;

    public SensorsInRoomsSource(int limit, double maxValueForRelevation) {
        this.limit = limit;
        this.generator = new DoublesGenerator((int) Math.round(maxValueForRelevation));
        this.roomIndex = new IntegersGenerator(ROOMS.length);
        this.stop = false;
    }

    @Override
    public void run(SourceContext<Tuple3<Integer, String, Double>> context) throws Exception {
        while (limit > 0 && !stop) {
            Tuple2<Integer, Double> next = generator.next();
            String room = ROOMS[roomIndex.next().f1];
            context.collect(Tuple3.of(next.f0, room, next.f1));
            limit--;
        }
    }

    @Override
    public void cancel() {
        this.stop = true;
    }
}
