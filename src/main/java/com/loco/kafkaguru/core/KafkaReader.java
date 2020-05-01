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
    private long minWait = 1000;
    private long waitPerMessage = 10;

    public KafkaReader(@NonNull String name, @NonNull String url) {
        kafkaInstance = new KafkaInstance(name, url);
    }

    public KafkaReader(@NonNull KafkaInstance instance) {
        this.kafkaInstance = instance;
    }

    private List<ConsumerRecord<String, String>> fetchMessages(@NonNull List<TopicPartition> topicPartitions,
            int maxMessageCount, long fetchFrom) {
        log.info("Getting messages");

        var topicMessages = new ArrayList<ConsumerRecord<String, String>>();
        if (topicPartitions.isEmpty()) {
            log.info("topicPartitions is empty");
            return topicMessages;
        }

        log.info("Obtaining offsets");
        List<PartitionOffset> partitionOffsets = kafkaInstance.getOffsets(topicPartitions);

        String topic = topicPartitions.get(0).topic();
        log.info("Fetching {} messages from topic {}", maxMessageCount, topic);

        var stopWatch = StopWatch.createStarted();

        synchronized (kafkaInstance) {
            // TODO ensure that all partitions are from the same topic?
            kafkaInstance.assign(topicPartitions);

            seek(partitionOffsets, fetchFrom, maxMessageCount);

            var wait = minWait + maxMessageCount * waitPerMessage;
            var batchSize = 0;
            do {
                ConsumerRecords<String, String> batch = kafkaInstance.poll(Duration.ofMillis(wait));
                batchSize = batch.count();

                for (var iterator = batch.iterator(); iterator.hasNext() && topicMessages.size() < maxMessageCount;) {
                    ConsumerRecord<String, String> record = iterator.next();
                    topicMessages.add(record);
                }
            } while (topicMessages.size() < maxMessageCount && batchSize > 0);

            log.info("Got {} messages from topic: {} in {} seconds.", topicMessages.size(), topic,
                    stopWatch.getTime(TimeUnit.SECONDS));
        }
        return topicMessages;
    }

    private void seek(List<PartitionOffset> partitionOffsets, long fetchFrom, int maxMessageCount) {
        switch ((int) fetchFrom) {
            case 0:
                seekToStart(partitionOffsets);
                break;
            case -1:
                seekToEnd(partitionOffsets, maxMessageCount);
                break;
            default:
                if (partitionOffsets.size() != 1) {
                    throw new IllegalArgumentException(
                            "loading from specific" + " offset is possible only if single partition is provided");
                }
                seekTo(partitionOffsets.get(0), fetchFrom);
                break;
        }
    }

    private void seekToStart(List<PartitionOffset> partitionOffsets) {
        for (PartitionOffset po : partitionOffsets) {
            seekTo(po, po.getStartOffset());
        }
    }

    private void seekToEnd(List<PartitionOffset> partitionOffsets, int maxMessageCount) {
        int remainingMessages = maxMessageCount;
        int remainingPartitions = partitionOffsets.size();

        for (PartitionOffset po : partitionOffsets) {
            if (po.getEndOffset() > po.getStartOffset()) {
                int messagesToFetch = remainingMessages / remainingPartitions;
                long startOffset = Math.max(po.getEndOffset() - messagesToFetch, po.getStartOffset());
                seekTo(po, startOffset);
                int availableMessages = (int) (po.getEndOffset() - startOffset);
                remainingMessages -= availableMessages;
            }
            --remainingPartitions;
        }
    }

    private void seekTo(PartitionOffset po, long offset) {
        kafkaInstance.seek(po.getTopicPartition(), offset);
    }

    public void getMessagesAsync(List<TopicPartition> topicPartitions, int limit, long fetchFrom,
            KafkaListener listener, Object sender) {
        log.info("In getMessagesAsync()");
        new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("calling getMessages()");
                try {
                    var messages = fetchMessages(topicPartitions, limit, fetchFrom);
                    log.info("obtained {} messages", messages.size());
                    listener.messagesReceived(messages, sender);
                } catch (Exception e) {
                    listener.messagesReceived(null, sender);
                }
            }
        }).start();
    }
}
