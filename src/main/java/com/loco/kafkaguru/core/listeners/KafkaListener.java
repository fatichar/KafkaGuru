package com.loco.kafkaguru.core.listeners;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public interface KafkaListener {
    public void topicsUpdated(Map<String, List<PartitionInfo>> topics);

    void messagesReceived(List<ConsumerRecord<String, String>> records, Object sender, int batchNumber,
            boolean moreToCome);

    void connected(boolean really);
}
