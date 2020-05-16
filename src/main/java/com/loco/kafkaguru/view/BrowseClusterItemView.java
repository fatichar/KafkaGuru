package com.loco.kafkaguru.view;

import com.loco.kafkaguru.controller.BrowseClusterItemViewController;
import javafx.fxml.FXMLLoader;
import javafx.scene.layout.AnchorPane;

import java.io.IOException;

public class BrowseClusterItemView extends AnchorPane {
    private BrowseClusterItemViewController controller;

    public BrowseClusterItemView(BrowseClusterItemViewController controller) {
        super();

        this.controller = controller;

        FXMLLoader loader = new FXMLLoader(getClass().getResource("BrowseClusterItemView.fxml"));
        loader.setRoot(this);
        loader.setController(controller);

        try {
            loader.load();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
