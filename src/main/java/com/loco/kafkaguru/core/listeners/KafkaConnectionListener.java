package com.loco.kafkaguru.core.listeners;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public interface KafkaConnectionListener {
    void connected(boolean really);
}

