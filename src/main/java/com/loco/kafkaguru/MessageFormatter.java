package com.loco.kafkaguru;

public interface MessageFormatter {
    public String name();
    public String format(byte[] data);
}
