import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class QueryForBF {


    private static final Logger log = LogManager.getLogger(QueryForBF.class);


    static double  queryForBF(String topici, String topico)
            throws ExecutionException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();

        String testtopic1i = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum("+ topici+")";
        String testtopic2 = "http://prometheus-operated:9090/api/v1/query?query=sum(" + topico + ")";




        List<URI> queries = new ArrayList<>();
        try {
            queries = Arrays.asList(
                    new URI(testtopic1i),
                    new URI(testtopic2)

            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        List<CompletableFuture<String>> results = queries.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());

        double[] rate = new double[2];

        int i=0;

        for (CompletableFuture<String> cf :  results) {
            //System.out.println(parseJson(cf.get()));
            rate[i++]= parseJson(cf.get());
        }

        if(rate[1]==0 || rate[0]== 0) return 0.0;


    try {
            return rate[1]/rate[0];
        } catch (Exception e) {
            log.info("looks like no data");
            return 0;
        }
    }




    static Double parseJson(String json ) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":
        // [{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        //{"status":"success","data":{"resultType":"vector","result":
        // [{"metric":{"__name__":"testtopic2","container":"cons1persec","endpoint":"brom",
        // "instance":"10.124.2.54:8080","job":"default/demoobservabilitypodmonitor","namespace":"default",
        // "pod":"cons1persec-6765c9946c-pl9f4","topic_to":"testtopic2"},"value":[1680516659.037,"37612"]}]}}
        try {
            JSONObject jsonObject = JSONObject.parseObject(json);
            JSONObject j2 = (JSONObject) jsonObject.get("data");
            JSONArray inter = j2.getJSONArray("result");
            JSONObject jobj = (JSONObject) inter.get(0);
            JSONArray jreq = jobj.getJSONArray("value");
            return Double.parseDouble(jreq.getString(1));
        } catch ( Exception e ) {
            // e.printStackTrace();
            log.info("looks like the service is still not discovered");
            return 0.0;
        }
    }
}
