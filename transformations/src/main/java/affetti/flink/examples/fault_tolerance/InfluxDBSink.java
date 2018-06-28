package affetti.flink.examples.fault_tolerance;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 22/08/17.
 */
public class InfluxDBSink<T extends Tuple> extends RichSinkFunction<T> implements CheckpointedFunction {
    private transient InfluxDB influxDB;
    private transient long counter = 0;
    private String dbName;

    private transient ListState<Long> checkpointedState;
    private transient Timer timer;
    private transient TimerTask writeThroughput;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool params = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        influxDB = InfluxDBFactory.connect(
                "http://" + params.get("influx-host", "localhost") + ":8086", "root", "root");
        dbName = parameters.getString("influx-db-name", "flink");
        influxDB.createDatabase(dbName);
        influxDB.setDatabase(dbName);
        influxDB.enableBatch(BatchOptions.DEFAULTS);

        timer = new Timer("throughput-timer");
        writeThroughput = new TimerTask() {
            long lastCounter = counter;

            @Override
            public void run() {
                synchronized (influxDB) {
                    long records = counter - lastCounter;
                    lastCounter = counter;
                    influxDB.write(Point.measurement("throughput")
                            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                            .addField("value", records)
                            .build());
                }
            }
        };
        timer.schedule(writeThroughput, 1000, 1000);
    }

    @Override
    public void close() throws Exception {
        super.close();
        writeThroughput.cancel();
        timer.cancel();
        timer.purge();
        influxDB.close();
    }

    @Override
    public void invoke(T t, Context context) throws Exception {
        counter++;

        synchronized (influxDB) {
            influxDB.write(Point.measurement("counter")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .addField("value", counter)
                    .build());

            String key = t.getField(0).toString();
            Object value = t.getField(1);
            influxDB.write(Point.measurement(key)
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .addField("value", value.toString())
                    .build());
        }
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
