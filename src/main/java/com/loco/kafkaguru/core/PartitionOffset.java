package com.loco.kafkaguru.core;

//import lombok.AllArgsConstructor;
//import lombok.Data;
import org.apache.kafka.common.TopicPartition;

//@Data
//@AllArgsConstructor
public class PartitionOffset {
    private TopicPartition topicPartition;

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public void setTopicPartition(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    private long startOffset;
    private long endOffset;

    public PartitionOffset(TopicPartition partition, Long startOffset, Long endOffset) {
        topicPartition = partition;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public String toString() {
        return topicPartition.toString() + ", [" + startOffset + " to " + endOffset + "]";
    }
}
