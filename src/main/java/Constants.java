import java.util.ArrayList;
import java.util.List;

public class Constants {


   static List<String> getQueriesArrival(String topicName) {

       List<String> queries = new ArrayList<>();

         String topic1ar = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22"+ topicName+ "%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";
         String topic1p0 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22"+ topicName+ "%22,partition=%220%22,namespace=%22default%22%7D%5B20s%5D))";
         String topic1p1 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22"+ topicName+ "%22,partition=%221%22,namespace=%22default%22%7D%5B20s%5D))";
         String topic1p2 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22"+ topicName+ "%22,partition=%222%22,namespace=%22default%22%7D%5B20s%5D))";
         String topic1p3 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22"+ topicName+ "%22,partition=%223%22,namespace=%22default%22%7D%5B20s%5D))";
         String topic1p4 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22"+ topicName+ "%22,partition=%224%22,namespace=%22default%22%7D%5B20s%5D))";

         queries.add(topic1ar);
         queries.add(topic1p0);
         queries.add(topic1p1);
         queries.add(topic1p2);
         queries.add(topic1p3);
         queries.add(topic1p4);
         return queries;

   }



   static  List<String> getQueriesLag(String topicName, String gname) {
        List<String> querieslag = new ArrayList<>();
         String topic1lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "sum(kafka_consumergroup_lag%7Bconsumergroup=%22"+gname+"%22,topic=%22"+ topicName+ "%22,namespace=%22default%22%7D)%20by%20(consumergroup,topic)";
         String topic1p0lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22"+gname+"%22,topic=%22"+ topicName+ "%22,partition=%220%22,namespace=%22default%22%7D";
         String topic1p1lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22"+gname+"%22,topic=%22"+ topicName+ "%22,partition=%221%22,namespace=%22default%22%7D";
         String topic1p2lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22"+gname+"%22,topic=%22"+  topicName+ "%22,partition=%222%22,namespace=%22default%22%7D";
         String topic1p3lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22"+gname+"%22,topic=%22"+ topicName+"%22,partition=%223%22,namespace=%22default%22%7D";
         String topic1p4lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22"+gname+"%22,topic=%22" + topicName+"%22,partition=%224%22,namespace=%22default%22%7D";

         querieslag.add(topic1lag);
        querieslag.add(topic1p0lag);
        querieslag.add(topic1p1lag);
        querieslag.add(topic1p2lag);
        querieslag.add(topic1p3lag);
        querieslag.add(topic1p4lag);

        return querieslag;
    }


    //  "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22, namespace=%22kubernetes_namespace%7D)%20by%20(consumergroup,topic)"
    //sum(kafka_consumergroup_lag{consumergroup=~"$consumergroup",topic=~"$topic", namespace=~"$kubernetes_namespace"}) by (consumergroup, topic)


}
