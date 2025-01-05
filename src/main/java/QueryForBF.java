import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import group.ConsumerGroup;
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

    static HttpClient client = HttpClient.newHttpClient();


    static double queryForBF(String topicp, String topicc)
            throws ExecutionException, InterruptedException {

        // e.g., testtopic1testtopic2_count/events_latency_count

        String bf = "http://prometheus-operated:9090/api/v1/query?query=" +
                "(avg(rate(" + topicp + topicc + "_count[5s])/rate(events_latency_" + topicp + "_count[5s])))";

        List<URI> queries = new ArrayList<>();
        try {
            queries = Arrays.asList(
                    new URI(bf)
                    //new URI(testtopic2)
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
        double lat =0;
        for (CompletableFuture<String> cf : results) {
            try {
                lat = Util.parseJsonLatency(cf.get());
                if (lat == 0.0 || Double.isNaN(lat)) return 0;

            } catch (Exception e) {
                return 0;
            }
        }
        return lat;
    }











}
