package com.loco.kafkaguru.view;

import com.loco.kafkaguru.controller.KafkaPaneController;
import javafx.fxml.FXMLLoader;
import javafx.scene.layout.AnchorPane;
import lombok.Getter;

import java.io.IOException;

@Getter
public class KafkaPane extends AnchorPane {
    private KafkaPaneController controller;

    public KafkaPane() {
        this("", "");
    }

    public KafkaPane(String kafkaName, String kafkaUrl) {
        super();

        FXMLLoader loader = new FXMLLoader(getClass().getResource("KafkaPane.fxml"));
        loader.setRoot(this);
        controller = new KafkaPaneController(kafkaName, kafkaUrl);
        loader.setController(controller);

        try {
            loader.load();
            controller.onLoadingFinished();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
