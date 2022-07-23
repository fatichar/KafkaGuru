package com.loco.kafkaguru.core;

import com.loco.kafkaguru.core.listeners.KafkaMessagesListener;
import com.loco.kafkaguru.pojo.FetchMessagesRequest;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Log4j2
@Getter
public class KafkaReader {
    private KafkaInstance kafkaInstance;
    private long maxWait = 10000;
    private long waitPerMessage = 10;
    private volatile Boolean abortReceived = false;

    public KafkaReader(@NonNull KafkaInstance instance) {
        this.kafkaInstance = instance;
    }

    private void fetchMore(
            List<TopicPartition> topicPartitions,
            int maxMessageCount,
            KafkaMessagesListener listener,
            Object sender) {
        var stopWatch = StopWatch.createStarted();

        synchronized (kafkaInstance) {
            var more = true;
            var totalCount = 0;
            int initialWait = 0000;
            var batch = new ArrayList<ConsumerRecord<String, byte[]>>();
            for (int batchNumber = 2; more; ) {
                log.info("iteration" + batchNumber);
                var localBatch = getNextBatch(maxMessageCount - totalCount, maxWait + initialWait);
                initialWait = 0;

                var batchSize = localBatch.size();
                totalCount += batchSize;
                log.info("obtained {} messages, total {}", batchSize, totalCount);
                batch.addAll(localBatch);

                more = (totalCount < maxMessageCount) && (batchSize > 0) && !abortReceived;
                if (!more || batch.size() % 20 == 0) {
                    synchronized (abortReceived) {
                        log.info("sent messages to UI");
                        listener.messagesReceived(batch, sender, batchNumber++, more);
                    }
                    batch = new ArrayList<>();
                }
            }
            abortReceived = false;

            log.info("Finished reading {} messages in {} seconds.", totalCount,
                    stopWatch.getTime(TimeUnit.SECONDS));
        }
    }

    private void fetchMessages(@NonNull List<TopicPartition> topicPartitions, int maxMessageCount, long fetchFrom,
            KafkaMessagesListener listener, Object sender) {
        log.info("Getting messages");

        if (topicPartitions.isEmpty()) {
            log.info("topicPartitions is empty");
            return;
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

            var more = true;
            var totalCount = 0;
            int initialWait = 0000;
            var batch = new ArrayList<ConsumerRecord<String, byte[]>>();
            for (int batchNumber = 1; more;) {
                var localBatch = getNextBatch(maxMessageCount - totalCount, maxWait + initialWait);
                batch.addAll(localBatch);
                initialWait = 0;

                var batchSize = localBatch.size();
                totalCount += batchSize;
                log.info("obtained {} messages, total {}", batchSize, totalCount);

                more = (totalCount < maxMessageCount) && (batchSize > 0) && !abortReceived;
                if (!more || batch.size() % 20 == 0) {
                    synchronized (abortReceived) {
                        listener.messagesReceived(batch, sender, batchNumber++, more);
                    }
                    batch = new ArrayList<>();
                }
            }
            abortReceived = false;

            log.info("Finished reading {} messages from topic: {} in {} seconds.", totalCount, topic,
                    stopWatch.getTime(TimeUnit.SECONDS));
        }
    }

    private void fetchMessagesFromEpoch(List<TopicPartition> topicPartitions, int maxMessageCount, long epochMilli,
            KafkaMessagesListener listener, Object sender) {

        String topic = topicPartitions.get(0).topic();
        log.info("Fetching {} messages from topic {}", maxMessageCount, topic);

        var stopWatch = StopWatch.createStarted();
        synchronized (kafkaInstance) {
            // TODO ensure that all partitions are from the same topic?
            kafkaInstance.assign(topicPartitions);
            var partitionTimestampMap = topicPartitions.stream().collect(Collectors.toMap(tp -> tp, tp -> epochMilli));
            Map<TopicPartition, OffsetAndTimestamp> partitionOffsetMap = kafkaInstance.getConsumer()
                    .offsetsForTimes(partitionTimestampMap);

            partitionOffsetMap.forEach((tp, offsetAndTimestamp) -> {
                    if (offsetAndTimestamp != null) {
                        kafkaInstance.getConsumer().seek(tp, offsetAndTimestamp.offset());
                    }
            });

            var more = true;
            var totalCount = 0;
            var batch = new ArrayList<ConsumerRecord<String, byte[]>>();
            for (int batchNumber = 1; more; ++batchNumber) {
                var localBatch = getNextBatch(maxMessageCount - totalCount, maxWait);

                var batchSize = localBatch.size();
                totalCount += batchSize;
                batch.addAll(localBatch);
                log.info("obtained {} messages, total {}", batchSize, totalCount);

                more = (totalCount < maxMessageCount) && (batchSize > 0) && !abortReceived;
                if (!more || batch.size() % 20 == 0) {
                    synchronized (abortReceived) {
                        listener.messagesReceived(batch, sender, batchNumber++, more);
                    }
                    batch = new ArrayList<>();
                }
            }
            abortReceived = false;

            log.info("Finished reading {} messages from topic: {} in {} seconds.", totalCount, topic,
                    stopWatch.getTime(TimeUnit.SECONDS));
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
            KafkaMessagesListener listener, Object sender) {
        log.info("In getMessagesAsync()");
        new Thread(() -> {
            log.info("calling getMessages()");
            try {
                fetchMessages(topicPartitions, limit, fetchFrom, listener, sender);
            } catch (Exception e) {
                listener.messagesReceived(null, sender, 0, false);
            }
        }).start();
    }

    public void getMoreAsync(List<TopicPartition> topicPartitions, int limit, KafkaMessagesListener listener, Object sender) {
        log.info("In getMoreAsync()");
        new Thread(() -> {
            log.info("calling fetchMore()");
            try {
                fetchMore(topicPartitions, limit, listener, sender);
            } catch (Exception e) {
                listener.messagesReceived(null, sender, 0, false);
            }
        }).start();
    }

    public void getMessagesFromTimeAsync(List<TopicPartition> topicPartitions, int limit, long epochMilli,
            KafkaMessagesListener listener, Object sender) {
        log.info("In getMessagesAsync()");
        new Thread(() -> {
            log.info("calling getMessages()");
            try {
                fetchMessagesFromEpoch(topicPartitions, limit, epochMilli, listener, sender);
            } catch (Exception e) {
                listener.messagesReceived(null, sender, 0, false);
            }
        }).start();
    }

    public void abortCurrentCalls() {
        synchronized (abortReceived) {
            abortReceived = true;
        }
    }
}
