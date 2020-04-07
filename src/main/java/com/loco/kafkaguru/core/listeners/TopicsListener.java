package com.loco.kafkaguru.core.listeners;

import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public interface TopicsListener {
    public void topicsUpdated(Map<String, List<PartitionInfo>> topics);
}
