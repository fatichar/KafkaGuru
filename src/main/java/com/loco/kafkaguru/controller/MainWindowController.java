package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.model.KafkaClusterInfo;
import com.loco.kafkaguru.view.KafkaPane;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TextInputDialog;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MainWindowController {
    @FXML
    private Button newTabButton;

    @FXML
    private TabPane tabPane;

    public void onNewConnection(ActionEvent actionEvent) {
        KafkaClusterInfo cluster = getKafkaClusterInfo();

        var controller = new KafkaPaneController(cluster);
        KafkaPane kafkaPane = new KafkaPane(controller);

        Tab tab = new Tab(cluster.getName());
        tab.setContent(kafkaPane);

        tabPane.getTabs().add(tab);
        tabPane.getSelectionModel().select(tab);
    }

    private KafkaClusterInfo getKafkaClusterInfo() {
//        var inputDialog = new TextInputDialog();
//        inputDialog.setHeaderText("Enter kafka URL");
//        inputDialog.showAndWait();
//
//        var kafkaUrl = inputDialog.getEditor().getText();
//        inputDialog.setHeaderText("Give a friendly name to this kafka instance");
//        inputDialog.getEditor().clear();
//        inputDialog.showAndWait();
//        var kafkaName = inputDialog.getEditor().getText();

//        return new KafkaClusterInfo(kafkaName, kafkaUrl);
        return new KafkaClusterInfo("Dev Sandbox", "ec2-54-226-137-43.compute-1.amazonaws.com:9092");
    }
}
