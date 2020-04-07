package com.loco.kafkaguru.viewmodel;

import com.loco.kafkaguru.core.KafkaInstance;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class ClusterNode implements AbstractNode {
    private KafkaInstance kafkaInstance;

    @Override
    public String toString(){
        return kafkaInstance.getName();
    }
}
