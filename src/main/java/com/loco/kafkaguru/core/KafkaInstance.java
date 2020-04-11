package com.loco.kafkaguru.core;

import com.loco.kafkaguru.core.listeners.KafkaListener;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;

public class KafkaInstance {
    @Getter private String name;
    @Getter private String url;
    private Properties properties;
    private KafkaConsumer<String, String> consumer;
    @Getter private Map<String, List<PartitionInfo>> topics;

    private Set<KafkaListener> kafkaListeners = new HashSet<>();

    private synchronized void setConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    public KafkaInstance(String name, String url) {
        this(name, url, new Properties());
    }

    public KafkaInstance(String name, String url, @NonNull Properties properties) {
        this.name = name;
        this.properties = properties;

        String[] parts = url.split(":");
        if (parts.length < 1) {
            throw new IllegalArgumentException("Url is empty");
        }
        if (parts.length > 2) {
            throw new IllegalArgumentException("Url should not have more than one ':' separator");
        }
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9092;
        this.url = createUrl(host, port);
    }

    public static String createUrl(String host, int port) {
        return host + ':' + port;
    }

    public void connect() throws KafkaException, UnknownHostException {
        setConsumer(createConsumer(url, properties));
        refreshTopics();
    }

    public void refreshTopics() throws KafkaException {
        synchronized (consumer) {
            var topics = consumer.listTopics(Duration.ofSeconds(5));
            setTopics(topics);
        }
    }

    private void setTopics(Map<String, List<PartitionInfo>> topics) {
        if (!topics.equals(this.topics)) {
            this.topics = topics;
            synchronized (kafkaListeners) {
                kafkaListeners.forEach(listener -> listener.topicsUpdated(topics));
            }
        }
    }

    public boolean addKafkaListener(KafkaListener listener) {
        synchronized (kafkaListeners) {
            return kafkaListeners.add(listener);
        }
    }

    public boolean removeKafkaListener(KafkaListener listener) {
        synchronized (kafkaListeners) {
            return kafkaListeners.remove(listener);
        }
    }

    private KafkaConsumer<String, String> createConsumer(String url, Properties properties)
            throws UnknownHostException {
        this.properties.putIfAbsent("bootstrap.servers", url);
        this.properties.putIfAbsent("client.id", InetAddress.getLocalHost().getHostName());
        this.properties.putIfAbsent("group.id", "kafka-tool");
        this.properties.putIfAbsent("auto.offset.reset", "earliest");
        this.properties.putIfAbsent("enable.auto.commit", "false");
        this.properties.putIfAbsent("max.poll.records", 1000);
        this.properties.putIfAbsent("key.deserializer", StringDeserializer.class);
        this.properties.putIfAbsent("value.deserializer", StringDeserializer.class);

        return new KafkaConsumer<String, String>(this.properties);
    }

    public void connectAsync() {
        new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    connect();
                                } catch (UnknownHostException e) {
                                    e.printStackTrace();
                                }
                            }
                        })
                .start();
    }

    public void assign(List<TopicPartition> topicPartitions) {
        synchronized (consumer) {
            consumer.assign(topicPartitions);
        }
    }

    public void seek(TopicPartition key, long startOffset) {
        synchronized (consumer) {
            consumer.seek(key, startOffset);
        }
    }

    public ConsumerRecords<String, String> poll(Duration timeout) {
        synchronized (consumer) {
            return consumer.poll(timeout);
        }
    }

    public Map<TopicPartition, Long> getStartOffsets(List<TopicPartition> topicPartitions) {
        synchronized (consumer) {
            return consumer.beginningOffsets(topicPartitions);
        }
    }

    public Map<TopicPartition, Long> getEndOffsets(List<TopicPartition> topicPartitions) {
        synchronized (consumer) {
            return consumer.endOffsets(topicPartitions);
        }
    }

    public List<PartitionOffset> getOffsets(List<TopicPartition> topicPartitions) {
        var offsets = new ArrayList<PartitionOffset>();

        Map<TopicPartition, Long> startOffsets;
        Map<TopicPartition, Long> endOffsets;

        synchronized (consumer) {
            startOffsets = getStartOffsets(topicPartitions);
            endOffsets = getEndOffsets(topicPartitions);
        }
        for (var entry : startOffsets.entrySet()) {
            var partition = entry.getKey();
            var startOffset = entry.getValue();
            var endOffset = endOffsets.get(partition);

            offsets.add(new PartitionOffset(partition, startOffset, endOffset));
        }
        return offsets;
    }
}
