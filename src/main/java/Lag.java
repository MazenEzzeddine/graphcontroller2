import group.ConsumerGroup;
import group.Partition;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Lag {
    private static final Logger log = LogManager.getLogger(Lag.class);

    public static AdminClient admin = null;
    static String BOOTSTRAP_SERVERS;
    static Map<TopicPartition, OffsetAndMetadata> committedOffsets;
    static long totalLag;

    //////////////////////////////////////////////////////////////////////////////

    public  static void readEnvAndCrateAdminClient() throws ExecutionException, InterruptedException {
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);
    }





    static void LagByOffsets(ConsumerGroup g) throws ExecutionException, InterruptedException {
        String topic = g.getInputTopic();
        //String cg = g.getKafkaName();

        committedOffsets = admin.listConsumerGroupOffsets(g.getKafkaName())
                .partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            requestLatestOffsets.put(new TopicPartition(topic, i), OffsetSpec.latest());
        }


        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();
        totalLag=0L;

        for (int i = 0; i < 5; i++) {
            TopicPartition t = new TopicPartition(topic, i);
            long latestOffset = latestOffsets.get(t).offset();
            long committedoffset = committedOffsets.get(t).offset();
            g.getTopicpartitions().get(i).setLag(latestOffset - committedoffset);
            totalLag += g.getTopicpartitions().get(i).getLag();

        }

        g.setTotalLag(totalLag);

    }



}
