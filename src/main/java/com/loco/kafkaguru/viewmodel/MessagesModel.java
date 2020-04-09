package com.loco.kafkaguru.viewmodel;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.ArrayList;
import java.util.List;

public class MessagesModel {
  @Getter
  private List<MessageModel> messages = new ArrayList<MessageModel>();

  public MessagesModel(List<ConsumerRecord<String, String>> records) {
    for (ConsumerRecord<String, String> record : records) {
      messages.add(new MessageModel(record));
    }
  }

  public void setMessages(List<MessageModel> messages) {
    this.messages = messages;
  }
}
