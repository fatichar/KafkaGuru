package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.core.KafkaInstance;
import com.loco.kafkaguru.core.KafkaReader;
import com.loco.kafkaguru.core.listeners.KafkaConnectionListener;
import com.loco.kafkaguru.model.KafkaClusterInfo;
import com.loco.kafkaguru.view.BrowseClusterItemView;
import com.loco.kafkaguru.view.BrowseClusterView;
import com.loco.kafkaguru.viewmodel.*;
import javafx.application.Platform;
import javafx.beans.property.*;
import javafx.beans.value.ChangeListener;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.PartitionInfo;

import java.net.URL;
import java.util.*;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

@Log4j2
public class KafkaViewController implements Initializable, KafkaConnectionListener, ClusterItemSelectionListener {

    private BrowseClusterViewController browseClusterController;
    private BrowseClusterItemViewController browseClusterItemController;

    // UI controls
    @FXML
    private SplitPane topicsMessagesPane;

    // data fields
    private KafkaClusterInfo cluster;
    private KafkaInstance kafkaInstance;

    private KafkaReader kafkaReader;
    private ClusterNode clusterNode;

    private ControllerListener parent;
    private Preferences preferences;

    // This is the node which is currently associated with the messages table.
    private boolean connected;
    private DoubleProperty topicMessageDividerPos;

    @Getter
    private String id;
    private ChangeListener<Number> dividerListener;

    public KafkaViewController(KafkaClusterInfo cluster, ControllerListener parent, Preferences preferences) {
        id = preferences.name();
        this.parent = parent;
        this.preferences = preferences;
        if (StringUtils.isEmpty(cluster.getName())) {
            throw new IllegalArgumentException("name is not specified");
        }
        if (StringUtils.isEmpty(cluster.getUrl())) {
            throw new IllegalArgumentException("url is not specified");
        }

        this.cluster = cluster;
    }

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        setupKafka();

        browseClusterController = new BrowseClusterViewController(preferences, parent, clusterNode);
        var browseClusterView = new BrowseClusterView(browseClusterController);
        topicsMessagesPane.getItems().add(browseClusterView);

        browseClusterItemController = new BrowseClusterItemViewController(kafkaReader, preferences);
        var browseClusterItemView = new BrowseClusterItemView(browseClusterItemController);
        topicsMessagesPane.getItems().add(browseClusterItemView);

        browseClusterController.addItemSelectionListener(browseClusterItemController);
        browseClusterController.addItemSelectionListener(this);

        topicMessageDividerPos = topicsMessagesPane.getDividers().get(0).positionProperty();
        dividerListener = (observable, oldValue, newValue) -> preferences.putDouble("topic_message_divider_pos",
                newValue.doubleValue());
        var lastDividerPos = preferences.getDouble("topic_message_divider_pos", 0.1);
        topicMessageDividerPos.set(lastDividerPos);

        // TODO report connection error
        kafkaInstance.connectAsync(this);
    }

    private void removeClusterNode() {
        parent.destroy(this);
        var removeCluster = new Alert(Alert.AlertType.ERROR,
                "Failed to fetch topics." + "\\nWould you like to remove the following cluster from your saved list?"
                        + "\n\n Cluster Name: " + kafkaInstance.getName(),
                ButtonType.YES, ButtonType.NO).showAndWait();
        if (removeCluster.orElse(ButtonType.NO).equals(ButtonType.YES)) {
            try {
                preferences.removeNode();

            } catch (BackingStoreException ex) {
                ex.printStackTrace();
            }
        }
    }

    private void setupKafka() {
        kafkaReader = new KafkaReader(cluster.getName(), cluster.getUrl());
        kafkaInstance = kafkaReader.getKafkaInstance();
        clusterNode = new ClusterNode(kafkaInstance);
    }

    @Override
    public void connected(boolean really) {
        if (really) {
            this.connected = true;
            kafkaReader.getKafkaInstance().refreshTopicsAsync(topics -> {
                topicMessageDividerPos.removeListener(dividerListener);
                var lastDividerPos = preferences.getDouble("topic_message_divider_pos", 0.1);
                browseClusterController.topicsUpdated(topics);
                topicMessageDividerPos.set(lastDividerPos);
                topicMessageDividerPos.addListener(dividerListener);
            });
        } else {
            Platform.runLater(() -> removeClusterNode());
        }
    }

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
    public void currentNodeChanged(AbstractNode selectedNode) {
        saveSelectionPreference(selectedNode);
    }

    private void saveSelectionPreference(AbstractNode selectedNode) {
        var selectedTopic = "";
        var selectedPartition = -1;
        if (selectedNode instanceof TopicNode) {
            selectedTopic = ((TopicNode) selectedNode).getTopic();
        } else {
            var partitionNode = (PartitionNode) selectedNode;
            selectedPartition = partitionNode.getPartition().partition();
            selectedTopic = partitionNode.getPartition().topic();
        }

        preferences.put("selected_topic", selectedTopic);
        preferences.putInt("selected_partition", selectedPartition);
    }
}
