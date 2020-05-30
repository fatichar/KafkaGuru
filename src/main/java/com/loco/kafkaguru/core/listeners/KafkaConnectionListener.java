package com.loco.kafkaguru.core.listeners;

import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public interface KafkaConnectionListener {
    void connectionFailed(String name);
    public void topicsUpdated(Map<String, List<PartitionInfo>> topics);

    void notifyUrlChange(String name, String oldUrl, String newUrl);

    void notifyNameChange(String id, String oldName, String newName);
}
