package com.loco.kafkaguru.core.listeners;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public interface KafkaMessagesListener {
    void messagesReceived(List<ConsumerRecord<String, byte[]>> records, Object sender, int batchNumber,
                          boolean moreToCome);
}
