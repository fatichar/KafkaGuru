package com.loco.kafkaguru.view;

import com.loco.kafkaguru.controller.BrowseClusterViewController;
import javafx.fxml.FXMLLoader;
import javafx.scene.layout.AnchorPane;

import java.io.IOException;

public class BrowseClusterView extends AnchorPane {
    private final BrowseClusterViewController controller;

    public BrowseClusterView(BrowseClusterViewController controller) {
        super();

        this.controller = controller;

        FXMLLoader loader = new FXMLLoader(getClass().getResource("BrowseClusterView.fxml"));
        loader.setRoot(this);
        loader.setController(controller);

        try {
            loader.load();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
