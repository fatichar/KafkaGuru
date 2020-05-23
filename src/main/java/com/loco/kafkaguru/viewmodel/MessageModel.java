package com.loco.kafkaguru.viewmodel;

import com.loco.kafkaguru.MessageFormatter;
import com.loco.kafkaguru.core.PluginLoader;
import javafx.beans.property.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

public class MessageModel {
    private static final int MAX_MESSAGE_SUMMARY_LEN = 200;

    private IntegerProperty index;
    private IntegerProperty partition;
    private LongProperty offset;
    private StringProperty key;
    private StringProperty messageSummary;
    private StringProperty messageBody;

    private SimpleObjectProperty<Date> timestamp;

    String timestampPattern = "E, dd MMM yyyy HH:mm:ss";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(timestampPattern);
    private MessageFormatter formatter;

    private ConsumerRecord<String, byte[]> record;

    public MessageModel(
            int index, ConsumerRecord<String, byte[]> record, MessageFormatter formatter) {
        this.record = record;
        this.formatter = formatter;
        this.index = new SimpleIntegerProperty(index);
        partition = new SimpleIntegerProperty(record.partition());
        offset = new SimpleLongProperty(record.offset());
        key = new SimpleStringProperty(record.key());
        var formatted = format(record.value());
        messageBody = new SimpleStringProperty(formatted);
        messageSummary = new SimpleStringProperty(summarize(formatted));

        timestamp = new SimpleObjectProperty<>(Date.from(Instant.ofEpochMilli(record.timestamp())));
    }

    private String summarize(String text) {
        var summary =
                text == null
                        ? ""
                        : text.length() > MAX_MESSAGE_SUMMARY_LEN
                                ? text.substring(0, MAX_MESSAGE_SUMMARY_LEN)
                                : text;

        while (summary.contains("\n") || summary.contains("\r")) {
            summary = summary.replace("\n", " ").replace("\r", " ");
        }
        summary = summary.replace("  ", " ");
        summary = summary.replace("  ", " ");

        return summary;
    }

    private String format(byte[] data) {
        if (data == null || formatter == null) {
            return new String(data);
        }

        try {
            var formatted = formatter.format(data);
            return formatted;
        } catch (Exception e) {
            return new String(data);
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

    public ConsumerRecord<String, byte[]> getRecord() {
        return record;
    }

    public void setFormatter(MessageFormatter formatter) {
        this.formatter = formatter;
        var formatted = format(record.value());
        messageBody = new SimpleStringProperty(formatted);
        messageSummary = new SimpleStringProperty(summarize(formatted));
    }
}
