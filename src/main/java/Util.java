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






    static void computeBranchingFactors(Graph g) throws ExecutionException, InterruptedException {

        int [][] A = g.getAdjMat();

     /*   for (int parent = 0; parent < A.length ;parent++) {
        for (int child = 0; child < A[parent].length; child++) {
            if (A[parent][child] == 1) {
                double bf =  QueryForBF.queryForBF(g.getVertex(parent).getG().getInputTopic()+"Total",
                        g.getVertex(parent).getG().getInputTopic() +     g.getVertex(child).getG().getInputTopic());
                g.setBF(parent,child, bf);
                log.info("BF[{}][{}]={}", parent, child, g.getBF()[parent][child]);
            }
        }
        }
*/


        for (int parent = 0; parent < A.length ;parent++) {
            for (int child = 0; child < A[parent].length; child++) {
                if (A[parent][child] == 1) {
                    g.setBF(parent,child, 1.0);
                    log.info("BF[{}][{}]={}", parent, child, g.getBF()[parent][child]);
                }
            }
        }
        g.setBF(0,1, 1.0);

        g.setBF(1,2, 1.0);






    }

}
