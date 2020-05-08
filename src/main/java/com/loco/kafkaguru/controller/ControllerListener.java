package com.loco.kafkaguru.controller;

import java.util.ArrayList;

public interface ControllerListener {
    void destroy(KafkaPaneController controller);

    void savePreference(ArrayList<String> nodeNames, String key, String value);

    String getPreference(ArrayList<String> nodes, String formatter);
}
