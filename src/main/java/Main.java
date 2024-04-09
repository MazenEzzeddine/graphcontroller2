import graph.Graph;
import graph.Vertex;
import group.ConsumerGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ExecutionException;


public class Main {

    private static final Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Lag.readEnvAndCrateAdminClient();
        initialize();
    }

    private static void initialize() throws InterruptedException, ExecutionException {
        Graph g = new Graph(3);

        ConsumerGroup g0 = new ConsumerGroup("testtopic1", 1,
                200, 0.5,
                "latency1", "testgroup1");
        ConsumerGroup g1 = new ConsumerGroup("testtopic2", 1,
                200, 0.5,
                "latency2", "testgroup2");
        ConsumerGroup g2 = new ConsumerGroup("testtopic3", 1,
                200, 0.5,
                "latency3", "testgroup3");


        g.addVertex(0, g0);
        g.addVertex(1, g1);
        g.addVertex(2, g2);

        // g.addVertex(2, g2);
        g.addEdge(0, 1);
        g.addEdge(1, 2);


        Stack<Vertex> ts = g.dfs(g.getVertex(0));
        List<Vertex> topoOrder = new ArrayList<>();
        //topological order
        while (!ts.isEmpty()) {
            topoOrder.add(ts.pop());
        }

/*
        log.info("Warming for 2 minutes seconds.");
        Thread.sleep(60*2*1000);*/
        log.info("Warming 20  seconds.");
        Thread.sleep(10 * 1000);

        while (true) {
            log.info("Querying Prometheus");
            Main.QueryingPrometheus(g, topoOrder);
            log.info("Sleeping for 5 seconds");
            log.info("******************************************");
            log.info("******************************************");
            Thread.sleep(30000);
        }
    }


    static void QueryingPrometheus(Graph g, List<Vertex> topoOrder)
            throws ExecutionException, InterruptedException {

       /* ArrivalRates.arrivalRateTopic1(g);
        ArrivalRates.arrivalRateTopic2(g.getVertex(1).getG());
        ArrivalRates.arrivalRateTopic2(g.getVertex(2).getG());*/

        Util.computeBranchingFactors(g);
        for (int m = 0; m < topoOrder.size(); m++) {
            log.info("Vertex/CG number {} in topo order is {}", m, topoOrder.get(m).getG());
            getArrivalRate2(g, m);
//            if (Duration.between(topoOrder.get(m).getG().getLastUpScaleDecision(),
//                    Instant.now()).getSeconds() > 3) {
//                //queryconsumergroups.QueryRate.queryConsumerGroup();
//                BinPack2.scaleAsPerBinPack(topoOrder.get(m).getG());
//            }

            BinPack2.scaleAsPerBinPack(topoOrder.get(m).getG());


        }

       // log.info("*********************************************");
    }


    /*static void getArrivalRate(Graph g, int m) throws ExecutionException, InterruptedException {

        int[][] A = g.getAdjMat();

        boolean grandParent = true;
        double totalArrivalRate = 0.0;
        for (int parent = 0; parent < A[m].length; parent++) {
            if (A[parent][m] == 1) {
                log.info( " {} {} is a prarent of {} {}", parent, g.getVertex(parent).getG() , m, g.getVertex(m).getG() );
                grandParent = false;
                totalArrivalRate += (g.getVertex(parent).getG().getTotalArrivalRate());
                if(g.getVertex(parent).getG().isScaled()) {
                    totalArrivalRate +=  (g.getVertex(parent).getG().getTotalLag()/(g.getVertex(parent).getG().getWsla()))
                            * g.getBF()[parent][m];
                }
            }
        }


        //attention only if scaled the lag of the parent shall be counted as arrival rate.
        // correct this.
        if (grandParent) {
            ArrivalRates.arrivalRateTopicGeneral(g.getVertex(m).getG(), false);
            log.info("Arrival rate of micorservice {} {}", m, g.getVertex(m).getG().getTotalArrivalRate());
        } else {
            g.getVertex(m).getG().setTotalArrivalRate(totalArrivalRate);
            ArrivalRates.arrivalRateTopicGeneral(g.getVertex(m).getG(), true);
            log.info("Arrival rate of micorservice {} {}", m, g.getVertex(m).getG().getTotalArrivalRate());
        }

    }
*/


    static void getArrivalRate2(Graph g, int m) throws ExecutionException, InterruptedException {

        int[][] A = g.getAdjMat();

        boolean grandParent = true;
        double totalArrivalRate = 0.0;
        for (int parent = 0; parent < A[m].length; parent++) {
            if (A[parent][m] == 1) {
                log.info( " {} {} is a prarent of {} {}", parent, g.getVertex(parent).getG() , m, g.getVertex(m).getG() );
                grandParent = false;
                totalArrivalRate += (g.getVertex(parent).getG().getTotalArrivalRate());
                if(g.getVertex(parent).getG().isScaled()) {
                    totalArrivalRate +=  (g.getVertex(parent).getG().getTotalLag()/(g.getVertex(parent).getG().getWsla()))
                            * g.getBF()[parent][m];
                }
            }
        }


        //attention only if scaled the lag of the parent shall be counted as arrival rate.
        // correct this.
        if (grandParent) {
            //ArrivalRates.arrivalRateTopicGeneral(g.getVertex(m).getG());
            ArrivalProducer.callForArrivals(g.getVertex(m).getG());
            Lag.LagByOffsets(g.getVertex(m).getG());
            log.info("Arrival rate of micorservice {} {}", m, g.getVertex(m).getG().getTotalArrivalRate());
        } else {
            g.getVertex(m).getG().setTotalArrivalRate(totalArrivalRate);
            //ArrivalRates.arrivalRateTopicGeneral(g.getVertex(m).getG(), true);
            Lag.LagByOffsets(g.getVertex(m).getG());
            log.info("Arrival rate of micorservice {} {}", m, g.getVertex(m).getG().getTotalArrivalRate());
        }

    }


    //attention only if scaled the lag of the parent shall be counted as arrival rate.


}
