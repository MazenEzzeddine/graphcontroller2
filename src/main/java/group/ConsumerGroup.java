package group;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConsumerGroup {
    private static final Logger log = LogManager.getLogger(ConsumerGroup.class);

    String inputTopic;
    String name;
    String kafkaName;
    Integer size;
    ArrayList<Partition> topicpartitions;
    double totalArrivalRate;
    double totalLag;
    double dynamicAverageMaxConsumptionRate;
    double wsla;
    Instant lastUpScaleDecision = Instant.now();

    public KubernetesClient k8s;

    public boolean isScaled() {
        return scaled;
    }


    public void setScaled(boolean scaled) {
        this.scaled = scaled;
    }

    boolean scaled;


    KafkaConsumerConfig kcg;
    List<Consumer> assignment;
    List<Consumer> currentAssignment;

    public KafkaConsumerConfig getKcg() {
        return kcg;
    }

    public void setKcg(KafkaConsumerConfig kcg) {
        this.kcg = kcg;
    }

    public List<Consumer> getAssignment() {
        return assignment;
    }

    public void setAssignment(List<Consumer> assignment) {
        this.assignment = assignment;
    }

    public List<Consumer> getCurrentAssignment() {
        return currentAssignment;
    }

    public void setCurrentAssignment(List<Consumer> currentAssignment) {
        this.currentAssignment = currentAssignment;
    }

    public List<Consumer> getTempAssignment() {
        return tempAssignment;
    }

    public void setTempAssignment(List<Consumer> tempAssignment) {
        this.tempAssignment = tempAssignment;
    }


    public static double getMu() {
        return mu;
    }

    public static void setMu(double mu) {
        ConsumerGroup.mu = mu;
    }

    List<Consumer> tempAssignment;

    public KafkaConsumer<byte[], byte[]> getMetadataConsumer() {
        return metadataConsumer;
    }

    public void setMetadataConsumer(KafkaConsumer<byte[], byte[]> metadataConsumer) {
        this.metadataConsumer = metadataConsumer;
    }

    private KafkaConsumer<byte[], byte[]> metadataConsumer;
    static double mu = 200.0;


    public ConsumerGroup(String inputTopic, Integer size,
                         double dynamicAverageMaxConsumptionRate,
                         double wsla, String name, String kname) {
        this.inputTopic = inputTopic;
        this.size = size;
        this.dynamicAverageMaxConsumptionRate = dynamicAverageMaxConsumptionRate;
        this.wsla = wsla;
        this.name = name;
        this.kafkaName = kname;
        topicpartitions = new ArrayList<>();
        for (int i = 0; i <= 4; i++) {
            topicpartitions.add(new Partition(i, 0, 0));
        }

        k8s = new KubernetesClientBuilder().build();
        scaled = false;
        kcg = new KafkaConsumerConfig("my-cluster-kafka-bootstrap:9092", inputTopic, kafkaName);
        Properties props = KafkaConsumerConfig.createProperties(kcg);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        metadataConsumer = new KafkaConsumer<>(props);
        assignment = new ArrayList<Consumer>();

        assignment.add(new Consumer("0", (long) (mu * wsla * .9),
                mu * .9));
        for (Partition p : topicpartitions) {
            assignment.get(0).assignPartition(p);

        }
        currentAssignment = assignment;
        tempAssignment = assignment;
    }


    public String getKafkaName() {
        return kafkaName;
    }

    public void setKafkaName(String kafkaName) {
        this.kafkaName = kafkaName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public double getDynamicAverageMaxConsumptionRate() {
        return dynamicAverageMaxConsumptionRate;
    }


    public double getWsla() {
        return wsla;
    }

    public Instant getLastUpScaleDecision() {
        return lastUpScaleDecision;
    }

    public void setLastUpScaleDecision(Instant lastUpScaleDecision) {
        this.lastUpScaleDecision = lastUpScaleDecision;
    }

    public String getInputTopic() {
        return inputTopic;
    }

    public ArrayList<Partition> getTopicpartitions() {
        return topicpartitions;
    }


    public double getTotalArrivalRate() {
        return totalArrivalRate;
    }

    public void setTotalArrivalRate(double totalArrivalRate) {
        this.totalArrivalRate = totalArrivalRate;
        // TODO
        for (int i = 0; i < 5; i++) {
            topicpartitions.get(i).setArrivalRate(totalArrivalRate / 5.0);
            log.info("Arrival rate for partition {} is {}", i, topicpartitions.get(i).getArrivalRate());
        }
    }

    public double getTotalLag() {
        return totalLag;
    }

    public void setTotalLag(double totalLag) {
        // TODO  TO BE OR NOT TO BE
        //  double max = Math.max(totalArrivalRate, dynamicAverageMaxConsumptionRate * size);
        // lag = lag + (arrivalsPerSec - consumedPerSec)
        // lag = max (0, lag)

        totalLag = Math.max(totalLag -  (dynamicAverageMaxConsumptionRate * size), 0);
        //totalLag = Math.max(totalLag - max, 0);
        this.totalLag = totalLag;


        //eventually equal lag per partition?
        //how about lagPerPartition = Math.max(lagPerPartition -  dynamicAverageMaxConsumptionRate, 0)
        //topicpartitions.get(i).setLag(lagPerPartition);
        for (int i = 0; i < 5; i++) {
            topicpartitions.get(i).setLag((long) (totalLag / 5));
            log.info("Lag for partition {} is {}", i, topicpartitions.get(i).getLag());
        }
        // log.info("Lag for partition {} is {}", i, topicpartitions.get(i).getLag());
    }
}
