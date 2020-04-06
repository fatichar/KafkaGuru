package com.loco.kafkaguru;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TreeView;
import javafx.scene.layout.Pane;
import lombok.Getter;
import lombok.Setter;
import net.rgielen.fxweaver.core.FxmlView;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.util.ResourceBundle;

@Component
@FxmlView("KafkaPane.fxml")
@Getter
@Setter
public class KafkaPaneController implements Initializable {
    @FXML private SplitPane topicsMessagesPane;

    @FXML private TreeView<?> topicsTree;

    @FXML private SplitPane messagesSplitPane;

    @FXML private TableView<?> messagesTable;

    @FXML private TextArea messageArea;

    public KafkaPaneController() {}

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {

    }
}
