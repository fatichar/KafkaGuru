package com.loco.kafkaguru.viewmodel;

import com.loco.kafkaguru.core.KafkaInstance;
//import lombok.AllArgsConstructor;
//import lombok.Data;

import java.util.List;

//@AllArgsConstructor
// @Data
public class ClusterNode implements AbstractNode {
    private KafkaInstance kafkaInstance;
    private List<MessageModel> messages;
    private List<TopicNode> topicNodes;

    public ClusterNode(KafkaInstance kafkaInstance) {
        this.kafkaInstance = kafkaInstance;
    }

    @Override
    public String toString() {
        return kafkaInstance.getName();
    }

    @Override
    public void addMessages(List<MessageModel> messages) {
    }

    @Override
    public List<MessageModel> getMessages() {
        return messages;
    }

    @Override
    public void setMessages(List<MessageModel> messages) {
        this.messages = messages;
    }

    public List<TopicNode> getTopicNodes() {
        return topicNodes;
    }

    public void setTopicNodes(List<TopicNode> topicNodes) {
        this.topicNodes = topicNodes;
    }
}
