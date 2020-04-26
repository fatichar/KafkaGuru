package com.loco.kafkaguru.viewmodel;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import lombok.Getter;
import java.util.List;

public class MessagesModel {
  @Getter
  private ObservableList<MessageModel> messages = FXCollections.observableArrayList();

  public void setMessages(List<MessageModel> messages) {
    if (messages == null) {
      this.messages.clear();
    } else {
      this.messages.setAll(messages);
    }
  }
}
