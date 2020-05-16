package com.loco.kafkaguru.view;

import com.loco.kafkaguru.controller.KafkaViewController;
import com.loco.kafkaguru.model.KafkaClusterInfo;
import javafx.fxml.FXMLLoader;
import javafx.scene.layout.AnchorPane;
import lombok.Getter;

import java.io.IOException;

@Getter
public class KafkaView extends AnchorPane {
    private KafkaViewController controller;

    public KafkaView(KafkaViewController controller) {
        super();

        this.controller = controller;

        FXMLLoader loader = new FXMLLoader(getClass().getResource("KafkaView.fxml"));
        loader.setRoot(this);
        loader.setController(controller);

        try {
            loader.load();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
