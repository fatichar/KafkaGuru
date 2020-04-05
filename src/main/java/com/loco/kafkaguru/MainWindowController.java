package com.loco.kafkaguru;

import java.io.IOException;

import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import net.rgielen.fxweaver.core.FxmlView;
import org.springframework.stereotype.Component;

@Component
@FxmlView("MainWindow.fxml")
public class MainWindowController {

    @FXML
    private Button newTabButton;

    @FXML
    private TabPane tabPane;

    @FXML
    private void switchToSecondary() throws IOException {
    }

    public void createNewTab(ActionEvent actionEvent) {
        ObservableList<Tab> tabs = tabPane.getTabs();
        tabs.add(new Tab("Tab 2"));
    }
}
