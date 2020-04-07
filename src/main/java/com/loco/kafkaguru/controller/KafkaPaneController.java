package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.core.KafkaInstance;
import com.loco.kafkaguru.core.listeners.TopicsListener;
import com.loco.kafkaguru.model.KafkaClusterInfo;
import com.loco.kafkaguru.viewmodel.AbstractNode;
import com.loco.kafkaguru.viewmodel.ClusterNode;
import com.loco.kafkaguru.viewmodel.TopicNode;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import lombok.Getter;
import lombok.Setter;
import net.rgielen.fxweaver.core.FxmlView;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

@Component
@FxmlView("KafkaPane.fxml")
@Getter
@Setter
public class KafkaPaneController implements Initializable, TopicsListener {
    // UI controls
    @FXML private SplitPane topicsMessagesPane;

    @FXML private TreeView<AbstractNode> topicsTree;

    @FXML private SplitPane messagesSplitPane;

    @FXML private TableView<?> messagesTable;

    @FXML private TextArea messageArea;

    // data fields
    private KafkaClusterInfo cluster;
    private KafkaInstance kafkaInstance;
    private Map<String, List<PartitionInfo>> topics;

    public KafkaPaneController(KafkaClusterInfo cluster) {
        this.cluster = cluster;
    }

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
    }

    public void loadingFinished() {
        kafkaInstance = new KafkaInstance(cluster.getName(), cluster.getUrl());
        kafkaInstance.addTopicsListener(this);
        var rootNode = new TreeItem<AbstractNode>(new ClusterNode(kafkaInstance));
        topicsTree.setRoot(rootNode);

        try {
            kafkaInstance.connect();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void topicsUpdated(Map<String, List<PartitionInfo>> newTopics) {
        try {
            if (topics == null || !newTopics.keySet().equals(topics.keySet())) {
                updateTopicsTree(newTopics);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateTopicsTree(Map<String, List<PartitionInfo>> newTopics) {
        var selectionModel = topicsTree.getSelectionModel();

        var rootNode = topicsTree.getRoot();
        int selectionRow = 0;
        if (topics != null) {
            selectionRow = selectionModel.getSelectedIndex();
        } else if (newTopics.keySet().size() > 0) {
            selectionRow = 1;
        }
        topics = newTopics;
        rootNode.getChildren().clear();

        List<TreeItem<AbstractNode>> topicNodes = createTopicNodes(topics);
        rootNode.getChildren().addAll(topicNodes);

        rootNode.setExpanded(true);

        selectionModel.select(selectionRow);
    }

    private List<TreeItem<AbstractNode>> createTopicNodes(Map<String, List<PartitionInfo>> topics) {
        var topicNodes =
                topics.entrySet().stream()
                        .map(entry -> createTopicNode(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());
        return topicNodes;
    }

    private TreeItem<AbstractNode> createTopicNode(String topic, List<PartitionInfo> partitions) {
        return new TreeItem<AbstractNode>(new TopicNode(null, topic, partitions));
    }
}
