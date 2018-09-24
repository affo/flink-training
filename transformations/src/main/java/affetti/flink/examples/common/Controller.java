package affetti.flink.examples.common;

import java.io.Closeable;

/**
 * Created by affo on 24/09/18.
 */
public interface Controller {
    void step();
    void close();
}
