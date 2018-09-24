package affetti.flink.examples.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by affo on 22/11/17.
 */
public class RealisticSensorsInRoomsSource extends RichParallelSourceFunction<Tuple2<String, Double>> {
    public static final int MAX_DELAY = 5;
    private NumberGenerator generator;
    private NumberGenerator roomIndex;
    private int numberOfRooms;
    private boolean verbose;
    private boolean stop;
    private int limit;
    private int baseIndex;
    private ControllerSupplier controllerSupplier;
    private transient Controller control;

    public RealisticSensorsInRoomsSource(
            int limit, double maxValueForRelevation, int numberOfRooms,
            boolean verbose, ControllerSupplier controllerSupplier) {
        this.limit = limit;
        this.generator = new NumberGenerator((int) Math.round(maxValueForRelevation));
        this.numberOfRooms = numberOfRooms;
        this.verbose = verbose;
        this.controllerSupplier = controllerSupplier;
        this.stop = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // assign a subset of the rooms
        int slice = numberOfRooms / getRuntimeContext().getNumberOfParallelSubtasks();
        this.baseIndex = slice * getRuntimeContext().getIndexOfThisSubtask();
        if (getRuntimeContext().getIndexOfThisSubtask() ==
                getRuntimeContext().getNumberOfParallelSubtasks() - 1) {
            // assign remaining rooms to last task
            slice += numberOfRooms % getRuntimeContext().getNumberOfParallelSubtasks();
        }

        this.roomIndex = new NumberGenerator(slice);
        this.control = controllerSupplier.supply(getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (control != null) {
            control.close();
        }
    }

    private String getRoom() {
        return "r" + (roomIndex.nextInt().f1 + baseIndex);
    }

    private void log(String s) {
        if (verbose) System.out.println(s);
    }

    @Override
    public void run(SourceContext<Tuple2<String, Double>> context) throws Exception {
        while (limit > 0 && !stop) {
            int batchSize = MAX_DELAY - limit;
            if (batchSize <= 0) {
                batchSize = MAX_DELAY;
            }

            List<Tuple2<Long, Double>> batch = new ArrayList<>(batchSize);
            long maxTS = -1;
            for (int i = 0; i < batchSize; i++) {
                Tuple2<Long, Double> next = generator.next();
                batch.add(next);
                maxTS = next.f0;
            }

            Collections.shuffle(batch);

            for (Tuple2<Long, Double> element : batch) {
                // send events only when the controller allows you
                control.step();

                Tuple2<String, Double> record = Tuple2.of(getRoom(), element.f1);
                context.collectWithTimestamp(record, element.f0);
                log(">>> Record emitted: " + record + ", TS: " + element.f0);
            }
            // we know that this batch is over, so we can emit the maxTimestamp reached for it
            context.emitWatermark(new Watermark(maxTS));
            log(">>> WM emitted: " + maxTS);

            limit -= batchSize;
        }
    }

    @Override
    public void cancel() {
        this.stop = true;
    }

    public interface ControllerSupplier extends Serializable {
        Controller supply(int subtaskIndex) throws Exception;
    }
}
