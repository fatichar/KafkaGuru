package com.loco.kafkaguru.viewmodel;

import com.loco.kafkaguru.MessageFormatter;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;

@Data
@Log4j2
public class TopicNode implements AbstractNode {
    private AbstractNode parent;
    private String topic;
    List<PartitionNode> partitions;

    private List<MessageModel> messages;
    private MessageFormatter formatter;

    public TopicNode(AbstractNode parent, String topic, List<PartitionInfo> partitions) {
        this.parent = parent;
        this.topic = topic;
        this.partitions = partitions.stream().map(p -> new PartitionNode(this, p)).collect(Collectors.toList());
    }

    public void setMessages(List<MessageModel> messages) {
        this.messages = messages;
         if (messages != null) {
             if (partitions.size() == 1){
                 partitions.get(0).setMessages(messages);
             }
        // partitions.forEach(p -> {
        // var subMessages = messages.stream().filter(m -> m.getPartition() ==
        // p.getPartition().partition())
        // .collect(Collectors.toList());
        // p.setMessages(subMessages);
        // });
         }
    }

    @Override
    public void addMessages(List<MessageModel> messages) {
        this.messages.addAll(messages);
        // if (messages != null) {
        // partitions.forEach(p -> {
        // var subMessages = messages.stream().filter(m -> m.getPartition() ==
        // p.getPartition().partition())
        // .collect(Collectors.toList());
        // p.addMessages(subMessages);
        // });
        // }
    }

    @Override
    public NodeType getType() {
        return NodeType.TOPIC;
    }

    public List<TopicPartition> getTopicPartitions() {
        return partitions.stream().map(p -> p.getTopicPartition()).collect(Collectors.toList());
    }

    public String toString() {
        return topic;
    }

    public boolean equals(Object other) {
        if (!(other instanceof PartitionNode)) {
            return false;
        }
        TopicNode otherNode = (TopicNode) other;
        if (!StringUtils.equals(topic, otherNode.getTopic())) {
            return false;
        }
        return true;
    }

    public int hashCode() {
        return topic.hashCode();
    }

    public AbstractNode getChildAt(int childIndex) {
        return partitions.get(childIndex);
    }

    public int getChildCount() {
        return partitions.size();
    }

    public AbstractNode getParent() {
        return parent;
    }

    public int getIndex(AbstractNode node) {
        return partitions.indexOf(node);
    }

    public boolean getAllowsChildren() {
        return true;
    }

    public boolean isLeaf() {
        return getChildCount() == 0;
    }

    public Enumeration children() {
        return Collections.enumeration(partitions);
    }

    public void setFormatter(MessageFormatter formatter) {
        log.info("In topic " + topic);
        log.info("existing formatter " + (this.formatter == null ? "null" : this.formatter.name()));
        log.info("Setting formatter " + formatter.name());
        if (this.formatter != formatter) {
            this.formatter = formatter;
            partitions.forEach(p -> p.setFormatter(formatter));
            reformatMessages();
        }
    }

    private void reformatMessages() {
        if (messages != null) {
            messages.forEach(msg -> msg.setFormatter(formatter));
        }
    }

    public MessageFormatter getFormatter() {
        return formatter;
    }
}
