package com.loco.kafkaguru.core;

import com.loco.kafkaguru.core.listeners.KafkaConnectionListener;
import com.loco.kafkaguru.model.KafkaClusterInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
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
import java.util.stream.Collectors;

@Log4j2
public class KafkaInstance {
    private Properties properties;
    @Getter
    private KafkaConsumer<String, byte[]> consumer;
    private List<KafkaConnectionListener> connectionListeners = new ArrayList<>();
    @Getter
    private KafkaClusterInfo clusterInfo;

    public KafkaInstance(KafkaClusterInfo clusterInfo) {
        this(clusterInfo, new Properties());
    }

    private synchronized void setConsumer(KafkaConsumer<String, byte[]> consumer) {
        this.consumer = consumer;
    }

    public KafkaInstance(KafkaClusterInfo clusterInfo, @NonNull Properties properties) {
        this.clusterInfo = clusterInfo;
        this.properties = properties;

        if (!StringUtils.isBlank(clusterInfo.getUrl())) {
            setUrl(clusterInfo.getUrl());
        }
    }

    private String createUrl(String givenUrl) {
        String[] parts = givenUrl.split(":");
        if (parts.length < 1) {
            throw new IllegalArgumentException("Url is empty");
        }
        if (parts.length > 2) {
            throw new IllegalArgumentException("Url should not have more than one ':' separator");
        }
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9092;

        return createUrl(host, port);
    }

    public static String createUrl(String host, int port) {
        return host + ':' + port;
    }

    private void connect() throws KafkaException, UnknownHostException {
        if (StringUtils.isBlank(clusterInfo.getUrl())) {
            throw new UnknownHostException("Kafka url is not specified");
        }
        setConsumer(createConsumer(clusterInfo.getUrl(), properties));
    }

    public Map<String, List<PartitionInfo>> refreshTopics() throws KafkaException {
        log.info("refreshing topics");
        synchronized (consumer) {
            var remainingTries = 3;
            do {
                try {
                    return consumer.listTopics(Duration.ofSeconds(5));
                } catch (KafkaException e) {
                    --remainingTries;
                    log.warn("Failed to obtain topics from Kafka. Remaining tries = " + remainingTries);
                    if (remainingTries == 0) {
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
        this.properties.putIfAbsent("group.id", "KafkaGuru");
        this.properties.putIfAbsent("auto.offset.reset", "earliest");
        this.properties.putIfAbsent("enable.auto.commit", "false");
        this.properties.putIfAbsent("max.poll.records", 1);
        this.properties.putIfAbsent("max.partition.fetch.bytes", 5000_000);
        this.properties.putIfAbsent("fetch.max.bytes", 1000_000);
        this.properties.putIfAbsent("key.deserializer", StringDeserializer.class);
        this.properties.putIfAbsent("value.deserializer", ByteArrayDeserializer.class);

        return new KafkaConsumer<String, byte[]>(this.properties);
    }

    public void connectAsync() {
        log.info("Starting connection thread");
        new Thread(() -> {
            log.info("Started connection thread");
            try {
                connect();
                var topics = refreshTopics();
                connectionListeners.forEach(cl -> cl.topicsUpdated(topics));
            } catch (KafkaException | UnknownHostException e) {
                log.error("Failed to connect to kafka ", e);
                connectionListeners.forEach(cl -> cl.connectionFailed(clusterInfo.getId()));
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

    public void addConnectionListener(KafkaConnectionListener listener) {
        connectionListeners.add(listener);
    }

    public void setUrl(String newUrl) {
        var oldUrl = clusterInfo.getUrl();
        String[] givenUrls = newUrl.split(",");
        var brokerUrls = Arrays.stream(givenUrls).map(url -> createUrl(url)).collect(Collectors.toList());
        var clusterUrl = brokerUrls.stream().reduce((allUrls, url) -> allUrls + ',' + url).orElse("");
        clusterInfo.setUrl(clusterUrl);
        connectionListeners.forEach(listener -> listener.notifyUrlChange(clusterInfo.getId(), oldUrl, clusterUrl));
    }

    public void setName(String newName) {
        var oldName = clusterInfo.getName();
        clusterInfo.setName(newName);
        connectionListeners.forEach(listener -> listener.notifyNameChange(clusterInfo.getId(), oldName, newName));
    }

    public String getName() {
        return clusterInfo.getName();
    }

    public String getUrl() {
        return clusterInfo.getUrl();
    }
}
