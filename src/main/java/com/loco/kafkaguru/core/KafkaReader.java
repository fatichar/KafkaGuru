package com.loco.kafkaguru.core;

import com.loco.kafkaguru.core.listeners.KafkaListener;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Log4j2
@Getter
public class KafkaReader {
    private KafkaInstance kafkaInstance;
    private long minWait = 5000;
    private long waitPerMessage = 20;

    public KafkaReader(@NonNull String name, @NonNull String url) {
        kafkaInstance = new KafkaInstance(name, url);
    }

    public KafkaReader(@NonNull KafkaInstance instance) {
        this.kafkaInstance = instance;
    }

    private List<ConsumerRecord<String, String>> fetchMessages(@NonNull List<TopicPartition> topicPartitions,
            int maxMessageCount) {
        log.info("Getting messages");

        List<ConsumerRecord<String, String>> topicMessages = new ArrayList<>();
        if (topicPartitions.isEmpty()) {
            log.info("topicPartitions is empty");
            return topicMessages;
        }

        log.info("Obtaining end offsets");
        List<PartitionOffset> partitionOffsets = kafkaInstance.getOffsets(topicPartitions);

        String topic = topicPartitions.get(0).topic();
        log.info("Fetching {} messages from topic {}", maxMessageCount, topic);

        var stopWatch = StopWatch.createStarted();

        synchronized (kafkaInstance) {
            int remainingMessages = maxMessageCount;
            int remainingPartitions = partitionOffsets.size();
            // TODO ensure that all partitions are from the same topic?
            kafkaInstance.assign(topicPartitions);
            for (PartitionOffset po : partitionOffsets) {
                if (po.getEndOffset() > po.getStartOffset()) {
                    int messagesToFetch = remainingMessages / remainingPartitions;
                    // fetch messages from single partition and fill them into topicMessages
                    int availableMessages = seek(po, messagesToFetch);
                    remainingMessages -= availableMessages;
                }
                --remainingPartitions;
            }

            var wait = minWait + maxMessageCount * waitPerMessage;
            ConsumerRecords<String, String> partitionRecords = kafkaInstance.poll(Duration.ofMillis(wait));

            for (ConsumerRecord<String, String> record : partitionRecords) {
                topicMessages.add(record);
            }

            log.info("Got {} messages from topic: {} in {} seconds.", topicMessages.size(), topic,
                    stopWatch.getTime(TimeUnit.SECONDS));
        }
        return topicMessages;
    }

    private int seek(PartitionOffset po, int messagesToFetch) {
        long startOffset = Math.max(po.getEndOffset() - messagesToFetch, po.getStartOffset());
        log.debug("Fetching {} messages from {}", messagesToFetch, po);

        kafkaInstance.seek(po.getTopicPartition(), startOffset);
        return (int) (po.getEndOffset() - startOffset);
    }

    public void getMessagesAsync(List<TopicPartition> topicPartitions, int i, KafkaListener listener, Object sender) {
        log.info("In getMessagesAsync()");
        new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("calling getMessages()");
                try {
                    var messages = fetchMessages(topicPartitions, i);
                    log.info("obtained {} messages", messages.size());
                    listener.messagesReceived(messages, sender);
                } catch (Exception e) {
                    listener.messagesReceived(null, sender);
                }
            }
        }).start();
    }
}
