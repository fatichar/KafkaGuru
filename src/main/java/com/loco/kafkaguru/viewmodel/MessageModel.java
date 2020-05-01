package com.loco.kafkaguru.viewmodel;

import javafx.beans.property.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

import org.json.JSONObject;

public class MessageModel {
    private static final int MAX_MESSAGE_SUMMARY_LEN = 100;

    private IntegerProperty index;
    private IntegerProperty partition;
    private LongProperty offset;
    private StringProperty key;
    private StringProperty messageSummary;
    private StringProperty messageBody;

    private SimpleObjectProperty<Date> timestamp;

    String timestampPattern = "E, dd MMM yyyy HH:mm:ss";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(timestampPattern);

    public MessageModel(int index, ConsumerRecord<String, String> record) {
        this.index = new SimpleIntegerProperty(index);
        partition = new SimpleIntegerProperty(record.partition());
        offset = new SimpleLongProperty(record.offset());
        key = new SimpleStringProperty(record.key());
        messageBody = new SimpleStringProperty(format(record.value()));
        messageSummary = new SimpleStringProperty(summarize(record.value()));

        timestamp = new SimpleObjectProperty<>(Date.from(Instant.ofEpochMilli(record.timestamp())));
    }

    private String summarize(String text) {
        var summary = text == null ? ""
                : text.length() > MAX_MESSAGE_SUMMARY_LEN ? text.substring(0, MAX_MESSAGE_SUMMARY_LEN) : text;

        while (summary.contains("\n") || summary.contains("\r")) {
            summary = summary.replace("\n", " ").replace("\r", " ");
        }

        return summary;
    }

    private String format(String text) {
        if (text == null) {
            return "";
        }

        try {
            String indented = new JSONObject(text).toString(4);
            return indented;
        } catch (Exception e) {
            return text;
        }
    }

    public int getIndex() {
        return index.get();
    }

    public int getPartition() {
        return partition.get();
    }

    public long getOffset() {
        return offset.get();
    }

    public String getKey() {
        return key.get();
    }

    public String getMessageSummary() {
        return messageSummary.get();
    }

    public String getTimestamp() {
        return simpleDateFormat.format(timestamp.get());
    }

    public String getMessageBody() {
        return messageBody.get();
    }
}
