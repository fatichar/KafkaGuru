package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.viewmodel.AbstractNode;

public interface ClusterItemSelectionListener {
    void currentNodeChanged(AbstractNode selectedNode);

    void messageFormatChanged(String topic);
}
