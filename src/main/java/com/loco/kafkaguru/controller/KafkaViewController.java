package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.core.KafkaInstance;
import com.loco.kafkaguru.core.KafkaReader;
import com.loco.kafkaguru.core.listeners.KafkaConnectionListener;
import com.loco.kafkaguru.view.BrowseClusterItemView;
import com.loco.kafkaguru.view.BrowseClusterView;
import com.loco.kafkaguru.viewmodel.*;
import javafx.beans.property.*;
import javafx.beans.value.ChangeListener;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.net.URL;
import java.util.*;

@Log4j2
public class KafkaViewController
        implements Initializable, KafkaConnectionListener, ClusterItemSelectionListener {
    @Getter private final String clusterId;

    @Getter
    //    private final String id = "";
    private BrowseClusterViewController browseClusterController;

    private BrowseClusterItemViewController browseClusterItemController;

    // UI controls
    @FXML private SplitPane topicsMessagesPane;

    // data fields
    private KafkaInstance kafkaInstance;

    @Getter private TabSettings settings;

    private DoubleProperty topicMessageDividerPos;

    private ChangeListener<Number> dividerListener;

    public KafkaViewController(
            KafkaInstance kafkaInstance, TabSettings settings, Map<String, String> topicFormats) {
        clusterId = kafkaInstance.getClusterInfo().getId();
        this.settings = settings;
        if (kafkaInstance == null) {
            throw new IllegalArgumentException("cluster is null");
        }

        this.kafkaInstance = kafkaInstance;
        kafkaInstance.addConnectionListener(this);
        var kafkaReader = new KafkaReader(kafkaInstance);

        browseClusterController =
                new BrowseClusterViewController(
                        kafkaReader, settings.getClusterViewSettings(), topicFormats);
        browseClusterItemController =
                new BrowseClusterItemViewController(
                        kafkaReader, settings.getCusterItemViewSettings());

        browseClusterController.addItemSelectionListener(browseClusterItemController);
        browseClusterController.addItemSelectionListener(this);
    }

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        var browseClusterView = new BrowseClusterView(browseClusterController);
        topicsMessagesPane.getItems().add(browseClusterView);

        var browseClusterItemView = new BrowseClusterItemView(browseClusterItemController);
        topicsMessagesPane.getItems().add(browseClusterItemView);

        topicMessageDividerPos = topicsMessagesPane.getDividers().get(0).positionProperty();
        dividerListener =
                (observable, oldValue, newValue) ->
                        settings.setDividerPosition(newValue.doubleValue());
        var lastDividerPos = settings.getDividerPosition();
        topicMessageDividerPos.set(lastDividerPos);

        // TODO report connection error
        if (!StringUtils.isEmpty(kafkaInstance.getUrl())) {
            kafkaInstance.connectAsync();
        }
    }

    private void removeClusterNode() {
        //        parent.destroy(this);
        var removeCluster =
                new Alert(
                                Alert.AlertType.ERROR,
                                "Failed to fetch topics."
                                        + "\\nWould you like to remove the following cluster from your saved list?"
                                        + "\n\n Cluster Name: "
                                        + kafkaInstance.getName(),
                                ButtonType.YES,
                                ButtonType.NO)
                        .showAndWait();
        if (removeCluster.orElse(ButtonType.NO).equals(ButtonType.YES)) {
            // TODO
        }
    }

    @Override
    public void connected(String clusterId, boolean really) {
        if (really) {
            kafkaInstance.refreshTopicsAsync(
                    topics -> {
                        topicMessageDividerPos.removeListener(dividerListener);
                        var lastDividerPos = settings.getDividerPosition();
                        browseClusterController.topicsUpdated(topics);
                        topicMessageDividerPos.set(lastDividerPos);
                        topicMessageDividerPos.addListener(dividerListener);
                    });
        } else {
            //            Platform.runLater(() -> removeClusterNode());
        }
    }

    @Override
    public void notifyUrlChange(String name, String oldUrl, String newUrl) {}

    @Override
    public void notifyNameChange(String id, String oldName, String newName) {}

    public void preferenceUpdated(ArrayList<String> nodeNames, String key, String value) {
        if (nodeNames.isEmpty()) {
            return;
        }
        nodeNames = new ArrayList<>(nodeNames);
        var nodeName = nodeNames.remove(0);
        switch (nodeName) {
            case "topics":
                var topic = nodeNames.remove(0);
                browseClusterController.topicPreferenceUpdated(nodeNames, topic, key, value);
                browseClusterItemController.topicPreferenceUpdated(nodeNames, topic, key, value);
                break;
            default:
                break;
        }
    }

    @Override
    public void currentNodeChanged(AbstractNode selectedNode) {}

    @Override
    public void messageFormatChanged(String topic) {}
}
