package com.loco.kafkaguru.core.listeners;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public interface KafkaListener {
    public void topicsUpdated(Map<String, List<PartitionInfo>> topics);
    public void messagesReceived(List<ConsumerRecord<String, String>> records);

    void connected(boolean really);
}
