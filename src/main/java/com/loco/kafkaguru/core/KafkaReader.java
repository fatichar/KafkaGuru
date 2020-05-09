package com.loco.kafkaguru.core;

import com.loco.kafkaguru.core.listeners.KafkaListener;
//import lombok.Getter;
//import lombok.NonNull;
//import lombok.extern.log4j.Log4j2;
// import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

//@Log4j2
//@Getter
public class KafkaReader {
    public KafkaInstance getKafkaInstance() {
        return kafkaInstance;
    }

    private KafkaInstance kafkaInstance;
    private long maxWait = 10000;
    private long waitPerMessage = 10;

    public KafkaReader(String name, String url) {
        kafkaInstance = new KafkaInstance(name, url);
    }

    public KafkaReader(KafkaInstance instance) {
        this.kafkaInstance = instance;
    }

    private void fetchMessages(List<TopicPartition> topicPartitions, int maxMessageCount, long fetchFrom,
            KafkaListener listener, Object sender) {
        // log.info("Getting messages");

        if (topicPartitions.isEmpty()) {
            // log.info("topicPartitions is empty");
            return;
        }

        // log.info("Obtaining offsets");
        List<PartitionOffset> partitionOffsets = kafkaInstance.getOffsets(topicPartitions);

        String topic = topicPartitions.get(0).topic();
        // log.info("Fetching {} messages from topic {}", maxMessageCount, topic);

        // var stopWatch = StopWatch.createStarted();

        synchronized (kafkaInstance) {
            // TODO ensure that all partitions are from the same topic?
            kafkaInstance.assign(topicPartitions);
            seek(partitionOffsets, fetchFrom, maxMessageCount);

            var more = true;
            var totalCount = 0;
            for (int batchNumber = 1; more; ++batchNumber) {
                var batch = getNextBatch(maxMessageCount - totalCount, maxWait);

                var batchSize = batch.size();
                totalCount += batchSize;
                // log.info("obtained {} messages, total {}", batchSize, totalCount);

                more = (totalCount < maxMessageCount) && (batchSize > 0);
                listener.messagesReceived(batch, sender, batchNumber, more);
            }

            // log.info("Finished reading {} messages from topic: {} in {} seconds.",
            // totalCount, topic,
            // stopWatch.getTime(TimeUnit.SECONDS));
        }
    }

    private ArrayList<ConsumerRecord<String, byte[]>> getNextBatch(int maxMessageCount, long wait) {
        var batch = kafkaInstance.poll(Duration.ofMillis(wait));

        var batchMessages = new ArrayList<ConsumerRecord<String, byte[]>>();
        for (var record : batch) {
            if (batchMessages.size() == maxMessageCount) {
                break;
            }
            batchMessages.add(record);
        }
        return batchMessages;
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
        // log.info("In getMessagesAsync()");
        new Thread(new Runnable() {
            @Override
            public void run() {
                // log.info("calling getMessages()");
                try {
                    fetchMessages(topicPartitions, limit, fetchFrom, listener, sender);
                } catch (Exception e) {
                    listener.messagesReceived(null, sender, 0, false);
                }
            }
        }).start();
    }
}
