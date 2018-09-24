package affetti.flink.examples.common;

import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;

/**
 * Created by affo on 24/09/18.
 */
public class NOPController implements Controller {
    @Override
    public void step() {
        // does nothing
    }

    @Override
    public void close() {

    }
}
