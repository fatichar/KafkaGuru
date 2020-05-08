module KakfaGuru {
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires lombok;
    requires org.apache.logging.log4j;
    requires org.mapstruct.processor;
    requires org.json;
    requires javafx.graphics;
    requires javafx.fxml;
    requires javafx.controls;
    requires org.apache.commons.lang3;
    requires kafka.clients;
    requires java.prefs;

    opens com.loco.kafkaguru.controller to javafx.fxml;
    opens com.loco.kafkaguru.view to javafx.fxml;
    opens com.loco.kafkaguru.core;
    opens com.loco.kafkaguru.model;
    opens com.loco.kafkaguru.viewmodel;
    opens com.loco.kafkaguru;
}