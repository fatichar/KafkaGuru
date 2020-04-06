package com.loco.kafkaguru;

import java.io.IOException;

import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import lombok.Getter;
import lombok.Setter;

//@Component
//@FxmlView("MainWindow.fxml")
@Getter
@Setter
public class MainWindowController {
    @FXML
    private Button newTabButton;

    @FXML
    private TabPane tabPane;

    public void onNewConnection(ActionEvent actionEvent) {
        ObservableList<Tab> tabs = tabPane.getTabs();
        Tab tab = new Tab("Connection 1");
        KafkaPane kafkaPane = new KafkaPane();
        tab.setContent(kafkaPane);
        tabs.add(tab);
        tabPane.getSelectionModel().select(tab);
    }
}
