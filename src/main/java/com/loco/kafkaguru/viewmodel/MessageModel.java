package com.loco.kafkaguru.viewmodel;

import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.beans.property.*;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;

public class MessageModel {
    private IntegerProperty partition;
    private LongProperty offset;
    private StringProperty key;
    private StringProperty messageBody;

    private StringProperty messageSummary;
    private SimpleObjectProperty<Date> timestamp;
    private ConsumerRecord<String, String> record;

    private ObjectMapper mapper = new ObjectMapper();

    public MessageModel(ConsumerRecord<String, String> record) {
        partition = new SimpleIntegerProperty(record.partition());
        offset = new SimpleLongProperty(record.offset());
        key = new SimpleStringProperty(record.key());
        messageBody = new SimpleStringProperty(format(record.value()));
        messageSummary = new SimpleStringProperty(summarize(record.value()));
        timestamp = new SimpleObjectProperty<>(Date.from(Instant.ofEpochMilli(record.timestamp())));
        this.record = record;
    }

    private String summarize(String text) {
        return text == null ? null : text.replace("\n", "\\n");
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

    public int getPartition() {
        return partition.get();
    }

    public IntegerProperty partitionProperty() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition.set(partition);
    }

    public long getOffset() {
        return offset.get();
    }

    public LongProperty offsetProperty() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset.set(offset);
    }

    public String getKey() {
        return key.get();
    }

    public StringProperty keyProperty() {
        return key;
    }

    public void setKey(String key) {
        this.key.set(key);
    }

    public String getMessageBody() {
        return messageBody.get();
    }

    public StringProperty messageBodyProperty() {
        return messageBody;
    }

    public void setMessageBody(String messageBody) {
        this.messageBody.set(messageBody);
    }

    public String getMessageSummary() {
        return messageSummary.get();
    }

    public StringProperty messageSummaryProperty() {
        return messageSummary;
    }

    public Date getTimestamp() {
        return timestamp.get();
    }

    public SimpleObjectProperty<Date> timestampProperty() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp.set(timestamp);
    }

    public ConsumerRecord<String, String> getRecord() {
        return record;
    }

    public void setRecord(ConsumerRecord<String, String> record) {
        this.record = record;
    }
}
