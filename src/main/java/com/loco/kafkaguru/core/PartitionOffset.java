package com.loco.kafkaguru.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.common.TopicPartition;

@Data
@AllArgsConstructor
public class PartitionOffset {
    private TopicPartition topicPartition;
    private long startOffset;
    private long endOffset;

    public String toString(){
        return topicPartition.toString() + ", [" + startOffset + " to " + endOffset + "]";
    }
}
