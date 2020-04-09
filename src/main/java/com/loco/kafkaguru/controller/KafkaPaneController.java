package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.core.KafkaInstance;
import com.loco.kafkaguru.core.KafkaReader;
import com.loco.kafkaguru.core.listeners.TopicsListener;
import com.loco.kafkaguru.model.KafkaClusterInfo;
import com.loco.kafkaguru.viewmodel.*;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.net.URL;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaPaneController implements Initializable, TopicsListener {
  // UI controls
  @FXML
  private SplitPane topicsMessagesPane;

  @FXML
  private TreeView<AbstractNode> topicsTree;

  @FXML
  private SplitPane messagesSplitPane;

  @FXML
  private TableView<MessageModel> messagesTable;
  @FXML
  private TableColumn<MessageModel, Integer> partitionColumn;
  @FXML
  private TableColumn<MessageModel, Long> offsetColumn;
  @FXML
  private TableColumn<MessageModel, String> keyColumn;
  @FXML
  private TableColumn<MessageModel, String> messageBodyColumn;
  @FXML
  private TableColumn<MessageModel, Date> timestampColumn;

  @FXML
  private TextArea messageArea;

  // data fields
  private KafkaClusterInfo cluster;
  private KafkaInstance kafkaInstance;
  private Map<String, List<PartitionInfo>> topics;
  private KafkaReader kafkaReader;
  private ClusterNode clusterNode;
  private MessagesModel messagesModel;

  public KafkaPaneController(KafkaClusterInfo cluster) {
    this.cluster = cluster;
  }

  @Override
  public void initialize(URL url, ResourceBundle resourceBundle) {
    setupKafka();
    setupTopicsTree();
    setupMessagesView();

    try {
      kafkaInstance.connect();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
  }

  private void setupMessagesView() {
    partitionColumn.setCellValueFactory(new PropertyValueFactory<>("partition"));
    offsetColumn.setCellValueFactory(new PropertyValueFactory<>("offset"));
    keyColumn.setCellValueFactory(new PropertyValueFactory<>("key"));
    messageBodyColumn.setCellValueFactory(new PropertyValueFactory<>("messageSummary"));
    timestampColumn.setCellValueFactory(new PropertyValueFactory<>("timestamp"));
  }

  private void setupTopicsTree() {
    var rootItem = new TreeItem<AbstractNode>(clusterNode);
    topicsTree.setRoot(rootItem);
    topicsTree.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<>() {
      @Override
      public void changed(ObservableValue<? extends TreeItem<AbstractNode>> observableValue,
          TreeItem<AbstractNode> oldItem, TreeItem<AbstractNode> newItem) {
        treeSelectionChanged(oldItem, newItem);
      }
    });
  }

  private void setupKafka() {
    kafkaReader = new KafkaReader(cluster.getName(), cluster.getUrl());
    kafkaInstance = kafkaReader.getKafkaInstance();
    kafkaReader.getKafkaInstance().addTopicsListener(this);
    clusterNode = new ClusterNode(kafkaInstance);
  }

  private void treeSelectionChanged(TreeItem<AbstractNode> oldItem, TreeItem<AbstractNode> newItem) {
    fetchMessages();
  }

  private void fetchMessages() {
    try {
      var topicPartitions = getSelectedTopicPartitions();

      var topicRecords = kafkaReader.getMessages(topicPartitions, 50);
      process(topicRecords);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void process(List<ConsumerRecord<String, String>> records) {
    MessagesModel newModel = new MessagesModel(records);
    if (messagesModel == null || !newModel.getMessages().equals(messagesModel.getMessages())) {
      int selectedRow = -1;
      if (messagesModel == null) {
        messagesModel = newModel;
        messagesTable.getItems().setAll(newModel.getMessages());
        if (newModel.getMessages().size() > 0) {
          selectedRow = 0;
        }
      } else {
        var selectionModel = messagesTable.getSelectionModel();
        selectedRow = selectionModel.getSelectedIndex();
        messagesModel.setMessages(newModel.getMessages());
        messagesTable.getItems().setAll(newModel.getMessages());

        selectionModel.select(selectedRow);
      }
    }
  }

  private List<TopicPartition> getSelectedTopicPartitions() {
    var selectedNode = getSelectedNode();

    final List<PartitionInfo> partitionInfos = new ArrayList<>();
    TopicNode topicNode = null;
    if (selectedNode instanceof TopicNode) {
      topicNode = (TopicNode) selectedNode;
      var partitions = topicNode.getPartitions().stream().map(p -> ((PartitionNode) p).getPartition())
          .collect(Collectors.toList());
      partitionInfos.addAll(partitions);
    } else if (selectedNode instanceof PartitionNode) {
      PartitionNode partitionNode = (PartitionNode) selectedNode;
      topicNode = (TopicNode) partitionNode.getParent();
      partitionInfos.add(partitionNode.getPartition());
    } else {
      return null;
    }

    final String topic = topicNode.getTopic();

    return partitionInfos.stream().map(pi -> new TopicPartition(topic, pi.partition())).collect(Collectors.toList());
  }

  private AbstractNode getSelectedNode() {
    var selectionModel = topicsTree.getSelectionModel();
    var selectedItem = selectionModel.getSelectedItem();
    return selectedItem.getValue();
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

    List<TreeItem<AbstractNode>> topicNodes = createTopicNodes(clusterNode, topics);
    rootNode.getChildren().addAll(topicNodes);

    rootNode.setExpanded(true);

    selectionModel.select(selectionRow);
  }

  private List<TreeItem<AbstractNode>> createTopicNodes(ClusterNode clusterNode,
      Map<String, List<PartitionInfo>> topics) {
    var topicNodes = topics.entrySet().stream()
        .map(entry -> createTopicNode(clusterNode, entry.getKey(), entry.getValue())).collect(Collectors.toList());
    return topicNodes;
  }

  private TreeItem<AbstractNode> createTopicNode(ClusterNode clusterNode, String topic,
      List<PartitionInfo> partitions) {
    var topicNode = new TopicNode(clusterNode, topic, partitions);
    var topicItem = new TreeItem<AbstractNode>(topicNode);

    var partitionNodes = createPartitionNodes(topicNode, partitions);
    topicItem.getChildren().addAll(partitionNodes);

    return topicItem;
  }

  private List<TreeItem<AbstractNode>> createPartitionNodes(TopicNode topicNode, List<PartitionInfo> partitions) {
    return partitions.stream().map(p -> createPartitionNode(topicNode, p)).collect(Collectors.toList());
  }

  private TreeItem<AbstractNode> createPartitionNode(TopicNode topicNode, PartitionInfo partitionInfo) {
    return new TreeItem<>(new PartitionNode(topicNode, partitionInfo));
  }
}
