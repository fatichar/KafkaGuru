package com.loco.kafkaguru.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaClusterInfo {
    private String id;
    private String name;
    private String url;

    public KafkaClusterInfo(String name, String url) {
        this(UUID.randomUUID().toString(), name, url);
    }
}
