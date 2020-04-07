package com.loco.kafkaguru.core;

import com.loco.kafkaguru.core.listeners.TopicsListener;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

@Getter
public class KafkaInstance {
    private String name;
    private String url;
    private Properties properties;
    private KafkaConsumer<String, String> consumer;
    private Map<String, List<PartitionInfo>> topics;

    private Set<TopicsListener> topicsListeners = new HashSet<>();

    public KafkaInstance(String name, String url){
        this(name, url, new Properties());
    }

    public KafkaInstance(String name, String url, @NonNull Properties properties){
        this.name = name;
        this.properties = properties;

        String[] parts = url.split(":");
        if (parts.length < 1) {
            throw new IllegalArgumentException("Url is empty");
        }
        if (parts.length > 2){
            throw new IllegalArgumentException("Url should not have more than one ':' separator");
        }
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9092;
        this.url = createUrl(host, port);
    }

    public static String createUrl(String host, int port){
        return host + ':' + port;
    }

    public void connect() throws UnknownHostException {
        consumer = createConsumer();
        refreshTopics();
    }

    public void refreshTopics(){
        var topics = consumer.listTopics();
        setTopics(topics);
    }

    private void setTopics(Map<String, List<PartitionInfo>> topics) {
        if (!topics.equals(this.topics)){
            this.topics = topics;
            topicsListeners.forEach(listener -> listener.topicsUpdated(topics));
        }
    }

    public boolean addTopicsListener(TopicsListener listener){
        return topicsListeners.add(listener);
    }

    public boolean removeTopicsListener(TopicsListener listener){
        return topicsListeners.remove(listener);
    }

    private KafkaConsumer<String, String> createConsumer() throws UnknownHostException {
        properties.putIfAbsent("bootstrap.servers", url);
        properties.putIfAbsent("client.id", InetAddress.getLocalHost().getHostName());
        properties.putIfAbsent("group.id", "kafka-tool");
        properties.putIfAbsent("auto.offset.reset", "earliest");
        properties.putIfAbsent("enable.auto.commit", "false");
        properties.putIfAbsent("max.poll.records", 1000);
        properties.putIfAbsent("key.deserializer", StringDeserializer.class);
        properties.putIfAbsent("value.deserializer", StringDeserializer.class);

        return new KafkaConsumer<String, String>(properties);
    }

    @Override
    public String toString(){
        return name;
    }
}