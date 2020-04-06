package com.loco.kafkaguru;

import com.loco.kafkaguru.KafkaPaneController;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.layout.AnchorPane;

import java.io.IOException;

public class KafkaPane extends AnchorPane {
    KafkaPaneController controller;

    public KafkaPane() {
        super();

        FXMLLoader loader = new FXMLLoader(getClass().getResource("KafkaPane.fxml"));
        loader.setRoot(this);
        controller = new KafkaPaneController();
        loader.setController(controller);

        Node n = null;
        try {
            n = loader.load();
//            this.getChildren().add(n);//don't forget to add the arrow to the custom control
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
