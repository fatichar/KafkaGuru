package com.loco.kafkaguru.core;

import com.loco.kafkaguru.core.KafkaReader;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;

import java.util.Arrays;
import java.util.List;

public class KafkaSubscriber {
    private KafkaReader reader;

    public KafkaSubscriber(@NonNull KafkaReader reader) {
        this.reader = reader;
    }

    public KafkaSubscriber(@NonNull String name, @NonNull String url) {
        this(new KafkaReader(name, url));
    }

    public boolean subscribe(String topic){
//        getConsumer().subscribe(Arrays.asList(topic));

        return true;
    }
}
