package affetti.flink.examples.common;

import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;

/**
 * Created by affo on 24/09/18.
 */
public class SocketController implements Controller {
    private Scanner control;

    public SocketController(String host, int port) throws IOException {
        Socket socket = new Socket("localhost", port);
        control = new Scanner(socket.getInputStream());
    }

    @Override
    public void step() {
        control.nextLine();
    }

    @Override
    public void close() {
        control.close();
    }
}
