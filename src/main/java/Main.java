import graph.Graph;
import graph.Vertex;
import group.ConsumerGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ExecutionException;


public class Main {

    private static final Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        Controller controller = new Controller();
        Thread controllerthread = new Thread(controller);

        AssignmentServer server = new AssignmentServer(5002);
        Thread serverthread = new Thread(server);
        serverthread.start();
        //Thread.sleep(60*5 * 1000);
        controllerthread.start();

    }


}
