import group.Consumer;
import group.ConsumerGroup;
import group.Partition;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;



public class BinPack2 {
    private static final Logger log = LogManager.getLogger(BinPack2.class);
    static float fup = 0.9f;//1.0f;//0.9f;
    static float fdown= 0.4f;//0.4f;



 /*   static KubernetesClient k8s1 = new KubernetesClientBuilder().build();
    static KubernetesClient k8s2 = new KubernetesClientBuilder().build();
    static KubernetesClient k8s3 = new KubernetesClientBuilder().build();*/









    public static void scaleAsPerBinPack(ConsumerGroup g) {
        log.info("Currently we have this number of consumers group {} {}", g.getKafkaName(), g.getSize());
        int neededsize = binPackAndScale(g);
        log.info("We currently need the following consumers for group1 (as per the bin pack) {}", neededsize);
        int replicasForscale = neededsize - g.getSize();
        if (replicasForscale > 0 ) {
            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale  group1 by {}", replicasForscale);
            g.setSize(neededsize);
            g.setScaled(true);
            g.setCurrentAssignment(List.copyOf(g.getAssignment()));
            g.setTempAssignment(List.copyOf(g.getAssignment()));

              new Thread(() -> {
                  g.k8s.apps().deployments().inNamespace("default").withName(g.getName()).scale(neededsize, false);
                  log.info("I have Upscaled group {} you should have {}", g.getKafkaName(), neededsize);

              }).start();

            g.setLastUpScaleDecision(Instant.now());
            return;

        }
        else {
            int neededsized = binPackAndScaled(g);
            int replicasForscaled =  g.getSize() - neededsized;
            if(replicasForscaled>0) {
                log.info("We have to downscale  group by {} {}", g.getKafkaName() ,replicasForscaled);
                g.setSize(neededsized);
               new Thread(() -> {
                    g.k8s.apps().deployments().inNamespace("default").withName(g.getName()).scale(neededsize, false);
                    log.info("I have Downscaled group {} you should have {}", g.getKafkaName(), neededsize);

                }).start();
                g.setCurrentAssignment(List.copyOf(g.getAssignment()));
                g.setLastUpScaleDecision(Instant.now());
                g.setScaled(true);
                return;
            }
        }  if (assignmentViolatesTheSLA2(g)) {
            g.getMetadataConsumer().enforceRebalance();
            g.setCurrentAssignment(List.copyOf(g.getTempAssignment()));
        }
        g.setScaled(false);
        log.info("===================================");
    }



    private static int binPackAndScale(ConsumerGroup g) {
        log.info(" shall we upscale group {}", g.getKafkaName());
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        List<Partition> parts = new ArrayList<>(g.getTopicpartitions());


        long maxLagCapacity;
        maxLagCapacity = (long) (g.getDynamicAverageMaxConsumptionRate() * g.getWsla() * fup);
        double dynamicAverageMaxConsumptionRate = g.getDynamicAverageMaxConsumptionRate()*fup;


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
                        String.format("%.2f", dynamicAverageMaxConsumptionRate));
                partition.setArrivalRate(dynamicAverageMaxConsumptionRate);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());

        while(true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(t)), maxLagCapacity,
                        dynamicAverageMaxConsumptionRate));
            }

            for (j = 0; j < parts.size() ; j++) {
                int i;
                Collections.sort(consumers);
                for (i = 0; i < consumerCount; i++) {

                    if (consumers.get(i).getRemainingLagCapacity() >= parts.get(j).getLag() &&
                            consumers.get(i).getRemainingArrivalCapacity() >= parts.get(j).getArrivalRate()) {
                        consumers.get(i).assignPartition(parts.get(j));
                        break;
                    }
                }
                if (i == consumerCount) {
                    consumerCount++;
                    break;
                }
            }
            if(j==parts.size())
                break;
        }
        log.info(" The BP up scaler recommended for group {} {}",g.getKafkaName(), consumers.size());

        g.setAssignment(consumers);

        return consumers.size();
    }





    private static int binPackAndScaled(ConsumerGroup g) {
        log.info(" shall we down scale group {} ", g.getKafkaName());
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        List<Partition> parts = new ArrayList<>(g.getTopicpartitions());
      double dynamicAverageMaxConsumptionRate = g.getDynamicAverageMaxConsumptionRate()*fdown;

        long maxLagCapacity;
        maxLagCapacity = (long) (dynamicAverageMaxConsumptionRate * g.getWsla());

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
                        String.format("%.2f", dynamicAverageMaxConsumptionRate));
                partition.setArrivalRate(dynamicAverageMaxConsumptionRate);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());
        while(true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(consumerCount)), maxLagCapacity,
                        dynamicAverageMaxConsumptionRate));
            }

            for (j = 0; j < parts.size() ; j++) {
                int i;
                Collections.sort(consumers);
                for (i = 0; i < consumerCount; i++) {

                    if (consumers.get(i).getRemainingLagCapacity() >= parts.get(j).getLag() &&
                            consumers.get(i).getRemainingArrivalCapacity() >= parts.get(j).getArrivalRate()) {
                        consumers.get(i).assignPartition(parts.get(j));
                        break;
                    }
                }
                if (i == consumerCount) {
                    consumerCount++;
                    break;
                }
            }
            if(j==parts.size())
                break;
        }
        g.setAssignment(consumers);

        log.info(" The BP down scaler recommended  for group {} {}",g.getKafkaName(), consumers.size());
        return consumers.size();
    }




    private static boolean assignmentViolatesTheSLA2(ConsumerGroup g) {
        List<Partition> partsReset = new ArrayList<>(g.getTopicpartitions());

        float   fraction = 0.9f;
        for (Partition partition : partsReset) {
            if (partition.getLag() > g.getDynamicAverageMaxConsumptionRate() * g.getWsla() * fraction) {
                partition.setLag((long) (g.getDynamicAverageMaxConsumptionRate() * g.getWsla() * fraction));
            }
        }

        for (Partition partition : partsReset) {
            if (partition.getArrivalRate() > g.getDynamicAverageMaxConsumptionRate() * fraction) {
                partition.setArrivalRate(g.getDynamicAverageMaxConsumptionRate() * fraction );
            }
        }
        for (Consumer cons : g.getCurrentAssignment()) {
            double sumPartitionsArrival = 0;
            double sumPartitionsLag = 0;
            for (Partition p : cons.getAssignedPartitions()) {
                sumPartitionsArrival += partsReset.get(p.getId()).getArrivalRate();
                sumPartitionsLag += partsReset.get(p.getId()).getLag();
            }

            if (sumPartitionsLag  > ( g.getWsla() * g.getDynamicAverageMaxConsumptionRate()  * .9f)
                    || sumPartitionsArrival > g.getDynamicAverageMaxConsumptionRate()* 0.9f) {
                return true;
            }
        }
        return false;
    }






}
