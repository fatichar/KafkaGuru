package com.loco.kafkaguru.model;

//import lombok.Data;

//@Data
public class KafkaClusterInfo {
    private final String name;
    private final String url;

    public KafkaClusterInfo(String name, String url) {
        this.name = name;
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public String getName() {
        return name;
    }
}
