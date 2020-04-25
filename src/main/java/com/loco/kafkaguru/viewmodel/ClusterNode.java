package com.loco.kafkaguru.viewmodel;

import com.loco.kafkaguru.core.KafkaInstance;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@AllArgsConstructor
@Data
public class ClusterNode implements AbstractNode {
  private KafkaInstance kafkaInstance;
  private List<MessageModel> messages;

  public ClusterNode(KafkaInstance kafkaInstance) {
    this(kafkaInstance, null);
  }

  @Override
  public String toString() {
    return kafkaInstance.getName();
  }
}
