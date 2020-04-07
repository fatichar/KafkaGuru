package com.loco.kafkaguru.viewmodel;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.time.Instant;

@Data
public class MessageModel {
    private int partition;
    private long offset;
    private String key;
    private String message;
    private String timeStamp;

    ObjectMapper mapper = new ObjectMapper();

    public MessageModel(ConsumerRecord<String, String> record) {
        key = record.key();
        message = format(record.value());
        offset = record.offset();
        partition = record.partition();
        timeStamp = Instant.ofEpochMilli(record.timestamp()).toString();
    }

    private String format(String text) {
        try {
            Object json = mapper.readValue(text, Object.class);
            String indented = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
            return indented;
        } catch (IOException e) {
            return text;
        }
    }

    public static int getColumnCount() {
        return 5;
    }

    public static Class<?> getColumnClass(int columnIndex) {
        if (0 == columnIndex) {
            return Long.class;
        }
        return String.class;
    }

    public static String getColumnName(int columnIndex) {
        switch (columnIndex){
            case 0:
                return "partition";
            case 1:
                return "offset";
            case 2:
                return "key";
            case 3:
                return "message";
            case 4:
                return "timeStamp";
            default:
                return "Invalid";
        }
    }

    public Object getValueAt(int columnIndex) {
        switch (columnIndex){
            case 0:
                return partition;
            case 1:
                return offset;
            case 2:
                return key;
            case 3:
                return message;
            case 4:
                return timeStamp;
            default:
                return null;
        }
    }
}
