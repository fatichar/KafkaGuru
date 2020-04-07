package com.loco.kafkaguru;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
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
    private String kafkaName;
    private String kafkaUrl;

    @FXML private SplitPane topicsMessagesPane;

    @FXML private TreeView<String> topicsTree;

    @FXML private SplitPane messagesSplitPane;

    @FXML private TableView<?> messagesTable;

    @FXML private TextArea messageArea;

    public KafkaPaneController(String kafkaName, String kafkaUrl) {
        this.kafkaName = kafkaName;
        this.kafkaUrl = kafkaUrl;
    }

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {

    }

    public void onLoadingFinished() {
        topicsTree.setRoot(new TreeItem<>(kafkaName));
    }
}
