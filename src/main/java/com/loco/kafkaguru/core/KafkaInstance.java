package com.loco.kafkaguru.core;

import com.loco.kafkaguru.core.listeners.KafkaConnectionListener;
import com.loco.kafkaguru.core.listeners.KafkaTopicsListener;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;

@Log4j2
public class KafkaInstance {
    @Getter
    private String name;
    @Getter
    private String url;
    private Properties properties;
    private KafkaConsumer<String, byte[]> consumer;

    private synchronized void setConsumer(KafkaConsumer<String, byte[]> consumer) {
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

    private void connect() throws KafkaException, UnknownHostException {
        setConsumer(createConsumer(url, properties));
    }

    public Map<String, List<PartitionInfo>> refreshTopics() throws KafkaException {
        synchronized (consumer) {
            var remainingTries = 3;
            do {
                try {
                    return consumer.listTopics(Duration.ofSeconds(5));
                } catch (KafkaException e) {
                    --remainingTries;
                    log.warn("Failed to obtain topics from Kafka. Remaining tries = " + remainingTries);
                    if (remainingTries == 0){
                        throw e;
                    }
                }
            } while (true);
        }
    }

    private KafkaConsumer<String, byte[]> createConsumer(String url, Properties properties)
            throws UnknownHostException {
        this.properties.putIfAbsent("bootstrap.servers", url);
        this.properties.putIfAbsent("client.id", InetAddress.getLocalHost().getHostName());
        this.properties.putIfAbsent("group.id", "kafka-tool");
        this.properties.putIfAbsent("auto.offset.reset", "earliest");
        this.properties.putIfAbsent("enable.auto.commit", "false");
        this.properties.putIfAbsent("max.poll.records", 100);
        this.properties.putIfAbsent("max.partition.fetch.bytes", 100_000);
        this.properties.putIfAbsent("key.deserializer", StringDeserializer.class);
        this.properties.putIfAbsent("value.deserializer", ByteArrayDeserializer.class);

        return new KafkaConsumer<String, byte[]>(this.properties);
    }

    public void connectAsync(KafkaConnectionListener listener) {
        log.info("Starting connection thread");
        new Thread(() -> {
            log.info("Started connection thread");
            try {
                connect();
                listener.connected(true);
            } catch (KafkaException | UnknownHostException e) {
                log.error("Failed to connect to kafka ", e);
                listener.connected(false);
            }
        }).start();
    }

    public void refreshTopicsAsync(KafkaTopicsListener listener) {
        log.info("Starting connection thread");
        new Thread(() -> {
            try {
                log.info("Obtaining topics from kafka ");
                var topics = refreshTopics();
                listener.topicsUpdated(topics);
                log.info("Obtained topics from kafka ");
            } catch (KafkaException e) {
                log.error("Failed to fetch topics from kafka for {}", url, e);
                listener.topicsUpdated(null);
            }
        }).start();
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

    public ConsumerRecords<String, byte[]> poll(Duration timeout) {
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
