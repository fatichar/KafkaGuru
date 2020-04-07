package com.loco.kafkaguru.viewmodel;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.PartitionInfo;

import javax.swing.tree.TreeNode;
import java.util.*;


@Data
public class TopicsViewModel implements AbstractNode {
    private List<AbstractNode> topics = new ArrayList<AbstractNode>();
    private String name;

    public TopicsViewModel(String host, Map<String, List<PartitionInfo>> topicsMap) {
        this.name = host;
        for (String topic : topicsMap.keySet()){
            var partitions = topicsMap.get(topic);
            topics.add(new TopicNode(this, topic, partitions));
        }
    }

    public String toString(){
        return name;
    }

    public boolean equals(Object other){
        TopicsViewModel otherModel = (TopicsViewModel) other;
        if (otherModel  == null){
            return false;
        }
        if (!StringUtils.equals(name, otherModel.getName())){
            return false;
        }
        return true;
    }

    public int hashCode(){
        return name.hashCode();
    }

    public AbstractNode getChildAt(int index) {
        return topics.get(index);
    }

    public int getChildCount() {
        return topics.size();
    }

    public TreeNode getParent() {
        return null;
    }

    public int getIndex(TreeNode node) {
        return topics.indexOf(node);
    }

    public boolean getAllowsChildren() {
        return true;
    }

    public boolean isLeaf() {
        return false;
    }

    public Enumeration children() {
        return Collections.enumeration(topics);
    }
}