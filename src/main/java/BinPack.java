import group.Consumer;
import group.ConsumerGroup;
import group.Partition;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BinPack {


    private static final Logger log = LogManager.getLogger(BinPack.class);

    public static void scaleAsPerBinPack(ConsumerGroup g) {
        log.info("Currently we have this number of consumers group {} {}", g.getKafkaName(), g.getSize());
        int neededsize = binPackAndScale(g);
        log.info("We currently need the following consumers for group1 (as per the bin pack) {}", neededsize);
        int replicasForscale = neededsize - g.getSize();
        if (replicasForscale > 0 ) {
            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale  group1 by {}", replicasForscale);
            g.setSize(neededsize);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName(g.getName()).scale(neededsize);
                log.info("I have Upscaled group {} you should have {}", g.getKafkaName(), neededsize);
                g.setLastUpScaleDecision(Instant.now());
            }
        }
        else {
            int neededsized = binPackAndScaled(g);
            int replicasForscaled =  g.getSize() - neededsized;
            if(replicasForscaled>0) {
                log.info("We have to downscale  group by {} {}", g.getKafkaName() ,replicasForscaled);
                g.setSize(neededsized);
                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    k8s.apps().deployments().inNamespace("default").withName(g.getName()).scale(neededsized);
                    log.info("I have downscaled group {} you should have {}", g.getKafkaName(), neededsized);
                }
                g.setLastUpScaleDecision(Instant.now());
            }
        }
        log.info("===================================");
    }



    private static int binPackAndScale(ConsumerGroup g) {
        log.info(" shall we upscale group {}", g.getKafkaName());
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        List<Partition> parts = new ArrayList<>(g.getTopicpartitions());


        long maxLagCapacity;
        maxLagCapacity = (long) (g.getDynamicAverageMaxConsumptionRate() * g.getWsla());
        consumers.add(new Consumer((String.valueOf(consumerCount)), maxLagCapacity, g.getDynamicAverageMaxConsumptionRate()));


        for (Partition partition : parts) {
            if (partition.getLag() > maxLagCapacity) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                        " we are truncating its lag", partition.getId(), partition.getLag(), maxLagCapacity);
                partition.setLag(maxLagCapacity);
            }
        }
        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > g.getDynamicAverageMaxConsumptionRate()) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f",  partition.getArrivalRate()),
                        String.format("%.2f", g.getDynamicAverageMaxConsumptionRate()));
                partition.setArrivalRate(g.getDynamicAverageMaxConsumptionRate());
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());

        Consumer consumer;


        for (Partition partition : parts) {
            int i;
            for ( i = 0; i < consumerCount; i++) {

                if ( consumers.get(i).getRemainingLagCapacity() >=  partition.getLag()  &&
                        consumers.get(i).getRemainingArrivalCapacity() >= partition.getArrivalRate()) {
                    consumers.get(i).assignPartition(partition);
                    break;
                }
            }
            if (i==consumerCount ) {
                consumerCount++;
                consumer = new Consumer((String.valueOf(consumerCount)), (long) (g.getDynamicAverageMaxConsumptionRate() * g.getWsla()),
                        g.getDynamicAverageMaxConsumptionRate());
                consumers.add(consumer);
                consumers.get(i).assignPartition(partition);

            }

        }
        log.info(" The BP up scaler recommended for group {} {}",g.getKafkaName(), consumers.size());
        return consumers.size();
    }





    private static int binPackAndScaled(ConsumerGroup g) {
        log.info(" shall we down scale group {} ", g.getKafkaName());
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        List<Partition> parts = new ArrayList<>(g.getTopicpartitions());
      double dynamicAverageMaxConsumptionRate = g.getDynamicAverageMaxConsumptionRate()*0.6;

        long maxLagCapacity;
        maxLagCapacity = (long) (dynamicAverageMaxConsumptionRate * g.getWsla());
        consumers.add(new Consumer((String.valueOf(consumerCount)), maxLagCapacity, dynamicAverageMaxConsumptionRate));

        //if a certain partition has a lag higher than R Wmax set its lag to R*Wmax
        // atention to the window
        for (Partition partition : parts) {
            if (partition.getLag() > maxLagCapacity) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                        " we are truncating its lag", partition.getId(), partition.getLag(), maxLagCapacity);
                partition.setLag(maxLagCapacity);
            }
        }
        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > dynamicAverageMaxConsumptionRate) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f",  partition.getArrivalRate()),
                        String.format("%.2f", g.getDynamicAverageMaxConsumptionRate()));
                partition.setArrivalRate(dynamicAverageMaxConsumptionRate);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());
        Consumer consumer;
        for (Partition partition : parts) {
            int i;
            for ( i = 0; i < consumerCount; i++) {

                if ( consumers.get(i).getRemainingLagCapacity() >=  partition.getLag()  &&
                        consumers.get(i).getRemainingArrivalCapacity() >= partition.getArrivalRate()) {
                    consumers.get(i).assignPartition(partition);
                    break;
                }
            }
            if(i == consumerCount) {
                consumerCount++;
                consumer = new Consumer((String.valueOf(consumerCount)), maxLagCapacity,
                        dynamicAverageMaxConsumptionRate);
                consumer.assignPartition(partition);
                consumers.add(consumer);
            }
        }

        log.info(" The BP down scaler recommended  for group {} {}",g.getKafkaName(), consumers.size());
        return consumers.size();
    }


}
