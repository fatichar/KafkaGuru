package com.loco.kafkaguru.viewmodel;

import javafx.beans.property.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Instant;
import java.util.Date;

import org.json.JSONObject;

public class MessageModel {
  private static final int MAX_MESSAGE_SUMMARY_LEN = 100;
  private IntegerProperty partition;
  private LongProperty offset;
  private StringProperty key;
  private StringProperty messageBody;

  private StringProperty messageSummary;
  private SimpleObjectProperty<Date> timestamp;
  private ConsumerRecord<String, String> record;

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
    var summary = text == null ? ""
            : text.length() > MAX_MESSAGE_SUMMARY_LEN ? text.substring(0, MAX_MESSAGE_SUMMARY_LEN)
            : text;

    while (summary.contains("\n") || summary.contains("\r")) {
      summary = summary.replace("\n", " ").replace("\r", " ");
    }

    return summary;
  }

  private String format(String text) {
    try {
      String indented = new JSONObject(text).toString(4);
      return indented;
    } catch (Exception e) {
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
