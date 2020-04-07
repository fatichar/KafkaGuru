package com.loco.kafkaguru.viewmodel;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableModel;
import java.util.ArrayList;
import java.util.List;

public class MessagesModel extends AbstractTableModel {
    @Getter
    private List<MessageModel> messages = new ArrayList<MessageModel>();
    private TableModelListener modelListener;

    public MessagesModel(List<ConsumerRecord<String, String>> records) {
        for (ConsumerRecord<String, String> record : records){
            messages.add(new MessageModel(record));
        }
    }

    public int getRowCount() {
        return messages.size();
    }

    public int getColumnCount() {
        return MessageModel.getColumnCount();
    }

    public String getColumnName(int columnIndex) {
        return MessageModel.getColumnName(columnIndex);
    }

    public Class<?> getColumnClass(int columnIndex) {
        return MessageModel.getColumnClass(columnIndex);
    }

    public boolean isCellEditable(int rowIndex, int columnIndex) {
        return false;
    }

    public Object getValueAt(int rowIndex, int columnIndex) {
        return messages.get(rowIndex).getValueAt(columnIndex);
    }

    public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
    }

    public void addTableModelListener(TableModelListener l) {
        modelListener = l;
    }

    public void removeTableModelListener(TableModelListener l) {
        if (modelListener == l){
            modelListener = null;
        }
    }

    public void setMessages(List<MessageModel> messages) {
        this.messages = messages;
        if (modelListener != null){
            modelListener.tableChanged(new TableModelEvent(this));
        }
        fireTableDataChanged();
    }
}
