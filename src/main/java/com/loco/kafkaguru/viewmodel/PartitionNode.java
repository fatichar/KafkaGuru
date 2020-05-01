package com.loco.kafkaguru.viewmodel;

import lombok.Data;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

@Data
public class PartitionNode implements AbstractNode {
    private final TopicPartition topicPartition;
    private AbstractNode parent;
    private PartitionInfo partition;
    private final String name;
    private List<MessageModel> messages;

    public PartitionNode(TopicNode parent, PartitionInfo partition) {
        this.parent = parent;
        this.partition = partition;
        name = "Partition " + partition.partition();
        topicPartition = new TopicPartition(partition.topic(), partition.partition());
    }

    public String toString() {
        return name;
    }

    public boolean equals(Object other) {
        PartitionNode otherNode = (PartitionNode) other;
        if (otherNode == null) {
            return false;
        }
        if (partition != otherNode.getPartition()) {
            return false;
        }
        return true;
    }

    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public void addMessages(List<MessageModel> messages) {
        this.messages.addAll(messages);
    }
}
