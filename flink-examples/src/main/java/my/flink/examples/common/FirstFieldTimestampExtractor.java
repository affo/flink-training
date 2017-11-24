package my.flink.examples.common;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * Uses the first field of a Tuple as TimeStampExtractor
 */
public class FirstFieldTimestampExtractor<T extends Tuple> extends AscendingTimestampExtractor<T> {

    @Override
    public long extractAscendingTimestamp(T tuple) {
        return Long.valueOf((Integer) tuple.getField(0));
    }
}
