package com.loco.kafkaguru.core;

import com.loco.kafkaguru.core.listeners.KafkaListener;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Log4j2
@Getter
public class KafkaReader {
    private KafkaInstance kafkaInstance;
    private long maxWait = 5;

    public KafkaReader(@NonNull String name, @NonNull String url) {
        kafkaInstance = new KafkaInstance(name, url);
    }

    public KafkaReader(@NonNull KafkaInstance instance) {
        this.kafkaInstance = instance;
    }

    public List<ConsumerRecord<String, String>> fetchMessages(
            @NonNull List<TopicPartition> topicPartitions, int maxMessageCount) {
        log.info("Getting messages");

        List<ConsumerRecord<String, String>> topicMessages = new ArrayList<>();
        if (topicPartitions.isEmpty()) {
            log.info("topicPartitions is empty");
            return topicMessages;
        }

        log.info("Obtaining end offsets");
        List<PartitionOffset> partitionOffsets = kafkaInstance.getOffsets(topicPartitions);

        synchronized (kafkaInstance) {
            int remainingMessages = maxMessageCount;
            int remainingPartitions = partitionOffsets.size();
            // TODO ensure that all partitions are from the same topic?
            String topic = topicPartitions.get(0).topic();
            log.info("Fetching {} messages from topic {}", maxMessageCount, topic);
            for (PartitionOffset po : partitionOffsets) {
                if (po.getEndOffset() > po.getStartOffset()) {
                    int messagesToFetch = remainingMessages / remainingPartitions;
                    //fetch messages from single partition and fill them into topicMessages
                    var messageCount = fetchMessages(topicMessages, po, messagesToFetch);
                    remainingMessages -= messageCount;
                }
                --remainingPartitions;
            }
            log.info("Got {} messages from topic: {}.", topicMessages.size(), topic);
        }
        return topicMessages;
    }

    private int fetchMessages(List<ConsumerRecord<String, String>> topicRecords, PartitionOffset po, int messagesToFetch) {
        long startOffset = Math.max(po.getEndOffset() - messagesToFetch, po.getStartOffset());
        log.debug("Fetching {} messages from {}", messagesToFetch, po);

        kafkaInstance.assign(Arrays.asList(po.getTopicPartition()));
        kafkaInstance.seek(po.getTopicPartition(), startOffset);

        ConsumerRecords<String, String> partitionRecords =
                kafkaInstance.poll(Duration.ofSeconds(maxWait));

        log.debug("Got {} records from {}.", partitionRecords.count(), po);
        for (ConsumerRecord<String, String> record : partitionRecords) {
            topicRecords.add(record);
        }
        return partitionRecords.count();
    }

    public void getMessagesAsync(
            List<TopicPartition> topicPartitions, int i, KafkaListener listener) {
        log.info("In getMessagesAsync()");
        new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                log.info("calling getMessages()");
                                try {
                                    var messages = fetchMessages(topicPartitions, i);
                                    log.info("obtained {} messages", messages.size());
                                    listener.messagesReceived(messages);
                                } catch (Exception e){
                                    listener.messagesReceived(null);
                                }
                            }
                        })
                .start();
    }
}
