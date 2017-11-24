package my.flink.examples.common;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Created by affo on 22/11/17.
 */
public class Printer<T> implements SinkFunction<T> {
    private final String prefix;

    public Printer(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void invoke(T t) throws Exception {
        System.out.println(">>> " + prefix + " - " + t.toString());
    }
}
