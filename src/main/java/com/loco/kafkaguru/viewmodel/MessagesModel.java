package com.loco.kafkaguru.viewmodel;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.List;

public class MessagesModel {
  @Getter
  private ObservableList<MessageModel> messages = FXCollections.observableArrayList();

  public MessagesModel() {
  }

  public void setRecords(List<ConsumerRecord<String, String>> records) {
    messages.clear();
    for (ConsumerRecord<String, String> record : records) {
      messages.add(new MessageModel(record));
    }
  }

  public void setMessages(List<MessageModel> messages) {
    this.messages.clear();
    if (messages != null) {
      this.messages.addAll(messages);
    }
  }
}
