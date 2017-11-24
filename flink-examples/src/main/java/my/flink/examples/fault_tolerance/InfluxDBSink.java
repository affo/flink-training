package my.flink.examples.fault_tolerance;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 22/08/17.
 */
public class InfluxDBSink<T extends Tuple> extends RichSinkFunction<T> implements CheckpointedFunction {
    private transient InfluxDB influxDB;
    private transient long counter = 0;

    private transient ListState<Long> checkpointedState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        influxDB = InfluxDBFactory.connect(
                "http://" + K.INFLUXDB_HOST + ":8086", "root", "root");
        influxDB.createDatabase(K.INFLUXDB_DBNAME);
        influxDB.setDatabase(K.INFLUXDB_DBNAME);
    }

    @Override
    public void close() throws Exception {
        super.close();
        influxDB.deleteDatabase(K.INFLUXDB_DBNAME);
        influxDB.close();
    }

    @Override
    public void invoke(T t) throws Exception {
        counter++;

        BatchPoints batchPoints = BatchPoints
                .database(K.INFLUXDB_DBNAME)
                .build();

        Point p1 = Point.measurement("counter")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField("value", counter)
                .build();

        String key = t.getField(0).toString();
        Object value = t.getField(1);
        Point p2 = Point.measurement(key)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField("value", value.toString())
                .build();

        batchPoints.point(p1);
        batchPoints.point(p2);
        influxDB.write(batchPoints);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointedState.clear();
        checkpointedState.add(counter);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> sd = new ListStateDescriptor<>("counter", Long.class);
        checkpointedState = context.getOperatorStateStore().getUnionListState(sd);

        if (context.isRestored()) {
            counter = checkpointedState.get().iterator().next();
        }
    }
}
