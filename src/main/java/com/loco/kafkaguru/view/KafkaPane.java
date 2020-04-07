package com.loco.kafkaguru.view;

import com.loco.kafkaguru.controller.KafkaPaneController;
import com.loco.kafkaguru.model.KafkaClusterInfo;
import javafx.fxml.FXMLLoader;
import javafx.scene.layout.AnchorPane;
import lombok.Getter;

import java.io.IOException;

@Getter
public class KafkaPane extends AnchorPane {
    private KafkaPaneController controller;
    private KafkaClusterInfo cluster;

    public KafkaPane(KafkaPaneController controller) {
        super();

        this.controller = controller;

        FXMLLoader loader = new FXMLLoader(getClass().getResource("KafkaPane.fxml"));
        loader.setRoot(this);
        loader.setController(controller);

        try {
            loader.load();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
