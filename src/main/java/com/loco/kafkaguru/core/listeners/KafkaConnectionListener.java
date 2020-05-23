package com.loco.kafkaguru.core.listeners;

public interface KafkaConnectionListener {
    void connected(String name, boolean really);

    void notifyUrlChange(String name, String oldUrl, String newUrl);

    void notifyNameChange(String id, String oldName, String newName);
}
