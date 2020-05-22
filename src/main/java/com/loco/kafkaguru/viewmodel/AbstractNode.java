package com.loco.kafkaguru.viewmodel;

import java.util.List;

public interface AbstractNode {
    List<MessageModel> getMessages();

    void setMessages(List<MessageModel> messages);

    void addMessages(List<MessageModel> messages);

    NodeType getType();
}
