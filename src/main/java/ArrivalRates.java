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

public class ArrivalRates {
    private static final Logger log = LogManager.getLogger(ArrivalRates.class);


    static void arrivalRateTopicGeneral(ConsumerGroup g) {

        String topic = g.getInputTopic();
        String cg = g.getKafkaName();
        List<String> arrivalqueries = Constants.getQueriesArrival(topic);
        List<String> lagqueries = Constants.getQueriesLag(topic, cg);

        HttpClient client = HttpClient.newHttpClient();
        List<URI> partitions2 = new ArrayList<>();
        try {
            partitions2 = Arrays.asList(
                    new URI(arrivalqueries.get(1)),
                    new URI(arrivalqueries.get(2)),
                    new URI(arrivalqueries.get(3)),
                    new URI(arrivalqueries.get(4)),
                    new URI(arrivalqueries.get(5))
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag2 = new ArrayList<>();
        try {
            partitionslag2 = Arrays.asList(
                    new URI(lagqueries.get(1)),
                    new URI(lagqueries.get(2)),
                    new URI(lagqueries.get(3)),
                    new URI(lagqueries.get(4)),
                    new URI(lagqueries.get(5))
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        /////////////////////////////////////////////////////////////

        List<CompletableFuture<String>> partitionsfutures2 = partitions2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        List<CompletableFuture<String>> partitionslagfuture2 = partitionslag2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partition2 = 0;
        double totalarrivalstopic2 = 0.0;
        double partitionArrivalRate2 = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures2) {
            try {
                partitionArrivalRate2 = Util.parseJsonArrivalRate(cf.get(), partition2);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            g.getTopicpartitions().get(partition2).setArrivalRate(partitionArrivalRate2);

            totalarrivalstopic2 += partitionArrivalRate2;
            partition2++;
        }
        g.setTotalArrivalRate(totalarrivalstopic2);
        log.info("totalArrivalRate for  topic  {} {}", g.getInputTopic() , totalarrivalstopic2);

        partition2 = 0;
        double totallag2 = 0.0;
        long partitionLag2 = 0L;

        for (CompletableFuture<String> cf : partitionslagfuture2) {
            try {
                partitionLag2 = Util.parseJsonArrivalLag(cf.get(), partition2).longValue();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            g.getTopicpartitions().get(partition2).setLag(partitionLag2);
            totallag2 += partitionLag2;
            partition2++;
        }
        log.info("totalLag for topic {} {}", g.getInputTopic(), totallag2);
        g.setTotalLag(totallag2);


    }




}
