package com.loco.kafkaguru.core;

import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Getter
public class KafkaReader {
    private KafkaInstance kafkaInstance;
    private long maxWait = 5;

    public KafkaReader(@NonNull String name, @NonNull String url) {
        kafkaInstance = new KafkaInstance(name, url);
    }

    public KafkaReader(@NonNull KafkaInstance instance){
        this.kafkaInstance = instance;
    }

    public List<ConsumerRecord<String, String>> getMessages(List<TopicPartition> topicPartitions, int maxMessageCount) {
        var consumer = kafkaInstance.getConsumer();
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

        int remainingMessages = maxMessageCount;
        int remainingPartitions = endOffsets.size();
        List<ConsumerRecord<String, String>> topicRecords = new ArrayList<>();
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            consumer.assign(Arrays.asList(entry.getKey()));
            int messagesToFetch = remainingMessages / remainingPartitions;

            long startOffset = Math.max(entry.getValue() - messagesToFetch, 0);
            consumer.seek(entry.getKey(), startOffset);
            ConsumerRecords<String, String> partitionRecords = consumer.poll(Duration.ofSeconds(maxWait));
            System.out.println("Got " + partitionRecords.count() + "records from partition " + entry.getKey().partition());
            for (ConsumerRecord<String, String> record : partitionRecords) {
                topicRecords.add(record);
            }
            remainingMessages -= messagesToFetch;
            --remainingPartitions;
        }
        return topicRecords;
    }

    public Map<String, List<PartitionInfo>> getTopics() {
        return kafkaInstance.getTopics();
    }

    public List<PartitionInfo> getTopic(@NonNull String topic) {
        return kafkaInstance.getTopics().get(topic);
    }
}
