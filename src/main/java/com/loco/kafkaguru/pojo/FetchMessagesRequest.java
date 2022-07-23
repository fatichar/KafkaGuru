package com.loco.kafkaguru.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.TopicPartition;

@Data
@AllArgsConstructor
@Builder
@NoArgsConstructor
public class FetchMessagesRequest {
    protected TopicPartition topicPartition;
    protected long offset;
    protected boolean backward;
}
