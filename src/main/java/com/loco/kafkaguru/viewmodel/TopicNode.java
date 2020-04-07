package com.loco.kafkaguru.viewmodel;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.PartitionInfo;

import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class TopicNode implements AbstractNode {
    private AbstractNode parent;
    private String topic;
    List<AbstractNode> partitions;

    public String toString(){
        return topic;
    }

    public TopicNode(AbstractNode parent, String topic, List<PartitionInfo> partitions) {
        this.parent = parent;
        this.topic = topic;
        this.partitions = partitions.stream().map(p -> new PartitionNode(this, p)).collect(Collectors.toList());
    }

    public boolean equals(Object other){
        TopicNode otherNode = (TopicNode) other;
        if (otherNode  == null){
            return false;
        }
        if (!StringUtils.equals(topic, otherNode.getTopic())){
            return false;
        }
        return true;
    }

    public int hashCode(){
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
}
