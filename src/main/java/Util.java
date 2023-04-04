import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import graph.Graph;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutionException;

public class Util {


    private static final Logger log = LogManager.getLogger(Util.class);


    static Double parseJsonArrivalRate(String json, int p) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":[{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject) jsonObject.get("data");
        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);
        JSONArray jreq = jobj.getJSONArray("value");
        return Double.parseDouble(jreq.getString(1));
    }


    static Double parseJsonArrivalLag(String json, int p) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":[{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject) jsonObject.get("data");
        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);
        JSONArray jreq = jobj.getJSONArray("value");
        return Double.parseDouble(jreq.getString(1));
    }


   /* static void computeBranchingFactors (Graph g) {
        int [][] A = g.getAdjMat();
        for (int m = 0; m < A.length; m++) {
            double parentsArrivalRate= 0;
            boolean issource= true;
            for (int parent = 0; parent < A[m].length; parent++) {
                if (A[parent][m] == 1) {
                    //log.info( " {} {} is a prarent of {} {}", parent, g.getVertex(parent).getG() , m, g.getVertex(m).getG() );
                    parentsArrivalRate += Math.min(g.getVertex(parent).getG().getTotalArrivalRate(),
                            g.getVertex(parent).getG().getSize()* g.getVertex(parent).getG().getDynamicAverageMaxConsumptionRate());
                    issource = false;

                    //BF[parent][m]= query prometheus for m input topic with tag as parent and
                    // dvide by query prometehus for input topic

                    // =   "http://prometheus-operated:9090/api/v1/query?query=testtopic1i"" +
                }
            }

            if (issource) {
                g.getVertex(m).getG().setBranchFactor(1.0);

            } else {
                g.getVertex(m).getG().setBranchFactor(g.getVertex(m).getG().getTotalArrivalRate()/parentsArrivalRate);
            }
        }
    }*/



    static void computeBranchingFactors(Graph g) throws ExecutionException, InterruptedException {

        int [][] A = g.getAdjMat();

        for (int parent = 0; parent < A.length ;parent++) {
        for (int child = 0; child < A[parent].length; child++) {
            if (A[parent][child] == 1) {



                double bf =  QueryForBF.queryForBF(g.getVertex(parent).getG().getInputTopic()+"Total",
                        g.getVertex(parent).getG().getInputTopic() +     g.getVertex(child).getG().getInputTopic());
                log.info("BF[{}][{}]={}", parent, child, bf );
            }
        }
        }

               /* if (parentsArrivalRate ==0) {
                   //ArrivalRates.arrivalRateTopicGeneral(g.getVertex(m).getG());
                   log.info("Arrival rate of micorservice {} {}",m,g.getVertex(m).getG().getTotalArrivalRate());
                } else {
                    g.getVertex(m).getG().setTotalArrivalRate(parentsArrivalRate*g.getVertex(m).getG().getBranchFactor());
                    log.info("Arrival rate of micorservice {} {}",m,g.getVertex(m).getG().getTotalArrivalRate());
                    log.info("Arrival rate of micorservice {} {}",m,parentsArrivalRate*g.getVertex(m).getG().getBranchFactor());

                }*/
    }

}
