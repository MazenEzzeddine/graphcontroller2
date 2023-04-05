import graph.Graph;
import graph.Vertex;
import group.ConsumerGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ExecutionException;


public class Main {

    private static final Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        initialize();
    }


    private static void initialize() throws InterruptedException, ExecutionException {
        Graph g = new Graph(3);

        ConsumerGroup g0 = new ConsumerGroup("testtopic1", 1, 191, 5,
                "cons1persec", "testgroup1");
        ConsumerGroup g1 = new ConsumerGroup("testtopic2", 1, 191, 5,
                "cons1persec2", "testgroup2");
        ConsumerGroup g2 = new ConsumerGroup("testtopic3", 1, 191, 5,
                "cons1persec3", "testgroup3");


        g.addVertex(0, g0);
        g.addVertex(1, g1);
        g.addVertex(2, g2);
        g.addEdge(0, 1);
        g.addEdge(1, 2);

        Stack<Vertex> ts = g.dfs(g.getVertex(0)); // 1 2 3 4 5
        List<Vertex> topoOrder = new ArrayList<>();
        //topological order
        while (!ts.isEmpty()) {
            topoOrder.add(ts.pop());
        }

/*
        log.info("Warming for 2 minutes seconds.");
        Thread.sleep(60*2*1000);*/
        log.info("Warming 30  seconds.");
        Thread.sleep(30 * 1000);

        //Thread.sleep(30);

        while (true) {
            log.info("Querying Prometheus");
            Main.QueryingPrometheus(g, topoOrder);
            log.info("Sleeping for 5 seconds");
            log.info("******************************************");
            log.info("******************************************");
            Thread.sleep(30000);
        }
    }


    static void QueryingPrometheus(Graph g, List<Vertex> topoOrder) throws ExecutionException, InterruptedException {




       /* ArrivalRates.arrivalRateTopic1(g);
        ArrivalRates.arrivalRateTopic2(g.getVertex(1).getG());
        ArrivalRates.arrivalRateTopic2(g.getVertex(2).getG());*/


        Util.computeBranchingFactors(g);

        for (int m = 0; m < topoOrder.size(); m++) {
            log.info("Vertex/CG number {} in topo order is {}", m, topoOrder.get(m).getG());
            getArrivalRate(g, m);
        }

        // ArrivalRates.arrivalRateTopic1(g);


       /* for (int i = 0; i < topoOrder.size(); i++) {
            log.info("Branch factor of ms  {} is {}", i, g.getVertex(i).getG().getBranchFactor());
            getArrivalRate(g, i);
            if (Duration.between(topoOrder.get(i).getG().getLastUpScaleDecision(), Instant.now()).getSeconds() > 15) {
                //queryconsumergroups.QueryRate.queryConsumerGroup();
                BinPack.scaleAsPerBinPack(topoOrder.get(i).getG());
            }
        }*/
    }


    static void getArrivalRate(Graph g, int m) throws ExecutionException, InterruptedException {

        int[][] A = g.getAdjMat();

        boolean grandParent = true;
        double totalArrivalRate = 0.0;
        for (int parent = 0; parent < A[m].length; parent++) {
            if (A[parent][m] == 1) {
                //log.info( " {} {} is a prarent of {} {}", parent, g.getVertex(parent).getG() , m, g.getVertex(m).getG() );
                //parentsArrivalRate += g.getVertex(parent).getG().getTotalArrivalRate();
                grandParent = false;
                totalArrivalRate += g.getVertex(parent).getG().getTotalArrivalRate() * g.getBF()[parent][m];
            }
        }

        if (grandParent) {
            ArrivalRates.arrivalRateTopicGeneral(g.getVertex(m).getG());
            log.info("Arrival rate of micorservice {} {}", m, g.getVertex(m).getG().getTotalArrivalRate());
        } else {
            g.getVertex(m).getG().setTotalArrivalRate(totalArrivalRate);

            log.info("Arrival rate of micorservice {} {}", m, g.getVertex(m).getG().getTotalArrivalRate());

        }

    }


}
