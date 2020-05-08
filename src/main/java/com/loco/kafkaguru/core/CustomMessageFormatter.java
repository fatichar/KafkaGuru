package com.loco.kafkaguru.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.loco.kafkaguru.MessageFormatter;

public class CustomMessageFormatter implements MessageFormatter {
    private Object foreignFormatter;
    private Method nameMethod;
    private Method formatMethod;

    public CustomMessageFormatter(Object foreignFormatter) throws NoSuchMethodException, SecurityException {
        this.foreignFormatter = foreignFormatter;

        nameMethod = foreignFormatter.getClass().getMethod("name");

        var arg = new Class[1];
        arg[0] = byte[].class;
        formatMethod = foreignFormatter.getClass().getMethod("format", arg);
    }

    @Override
    public String name() {
        try {
            return nameMethod.invoke(foreignFormatter).toString();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String format(byte[] data) {
        try {
            return formatMethod.invoke(foreignFormatter, data).toString();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }
}
