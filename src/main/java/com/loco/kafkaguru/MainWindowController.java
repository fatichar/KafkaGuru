package com.loco.kafkaguru;

import java.io.IOException;

import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TextInputDialog;
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
        var inputDialog = new TextInputDialog();
        inputDialog.setHeaderText("Enter kafka URL");
        inputDialog.showAndWait();

        var kafkaUrl = inputDialog.getEditor().getText();
        inputDialog.setHeaderText("Give a friendly name to this kafka instance");
        inputDialog.showAndWait();
        var kafkaName = inputDialog.getEditor().getText();

        ObservableList<Tab> tabs = tabPane.getTabs();
        Tab tab = new Tab(kafkaName);
        KafkaPane kafkaPane = new KafkaPane(kafkaName, kafkaUrl);
        tab.setContent(kafkaPane);
        tabs.add(tab);
        tabPane.getSelectionModel().select(tab);
    }
}
