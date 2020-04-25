package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.core.KafkaInstance;
import com.loco.kafkaguru.core.KafkaReader;
import com.loco.kafkaguru.core.listeners.KafkaListener;
import com.loco.kafkaguru.model.KafkaClusterInfo;
import com.loco.kafkaguru.viewmodel.*;
import javafx.application.Platform;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.net.URL;
import java.util.*;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;
import java.util.stream.Collectors;

@Log4j2
public class KafkaPaneController implements Initializable, KafkaListener {
  // UI controls
  @FXML
  private SplitPane topicsMessagesPane;
  @FXML
  private TreeView<AbstractNode> topicsTree;
  @FXML
  private SplitPane messagesSplitPane;

  // messages toolbar
  @FXML
  private Button refreshButton;
  @FXML
  private TextField filterField;

  // messages table
  @FXML
  private TableView<MessageModel> messagesTable;
  @FXML
  private TableColumn<MessageModel, Integer> partitionColumn;
  @FXML
  private TableColumn<MessageModel, Long> offsetColumn;
  @FXML
  private TableColumn<MessageModel, String> keyColumn;
  @FXML
  private TableColumn<MessageModel, String> messageSummaryColumn;
  @FXML
  private TableColumn<MessageModel, Date> timestampColumn;

  // message display
  @FXML
  private TextArea messageArea;

  // data fields
  private KafkaClusterInfo cluster;
  private KafkaInstance kafkaInstance;

  private Map<String, List<PartitionInfo>> topics;
  private KafkaReader kafkaReader;
  private ClusterNode clusterNode;
  private MessagesModel messagesModel;

  private Preferences preferences;
  private boolean followTreeSelection = true;
  private AbstractNode currentTopicNode;

  public KafkaPaneController(Preferences preferences) {
    this.preferences = preferences;
    var name = preferences.get("name", null);
    var url = preferences.get("url", null);
    if (StringUtils.isEmpty(name)) {
      throw new IllegalArgumentException("name is not specified");
    }
    if (StringUtils.isEmpty(url)) {
      throw new IllegalArgumentException("url is not specified");
    }

    cluster = new KafkaClusterInfo(name, url);
  }

  public String getName() {
    return cluster == null ? "" : cluster.getName();
  }

  @Override
  public void initialize(URL url, ResourceBundle resourceBundle) {
    setupKafka();
    setupTopicsTree();
    setupMessagesView();

    // TODO report connection error
    kafkaInstance.connectAsync(this);
  }

  // TODO use this
  private void removeClusterNode() {
    var removeCluster = new Alert(Alert.AlertType.ERROR,
        "Failed to fetch topics.\nWould you like to remove this cluster?", ButtonType.YES, ButtonType.NO).showAndWait();
    if (removeCluster.orElse(ButtonType.NO).equals(ButtonType.YES)) {
      try {
        preferences.removeNode();
      } catch (BackingStoreException ex) {
        ex.printStackTrace();
      }
    }
  }

  private void setupMessagesView() {
    setupMessagesToolbar();

    partitionColumn.setCellValueFactory(new PropertyValueFactory<>("partition"));
    offsetColumn.setCellValueFactory(new PropertyValueFactory<>("offset"));
    keyColumn.setCellValueFactory(new PropertyValueFactory<>("key"));
    messageSummaryColumn.setCellValueFactory(new PropertyValueFactory<>("messageSummary"));
    timestampColumn.setCellValueFactory(new PropertyValueFactory<>("timestamp"));

    messagesTable.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<MessageModel>() {
      @Override
      public void changed(ObservableValue<? extends MessageModel> observableValue, MessageModel oldMessage,
          MessageModel newMessage) {
        displayMessage(newMessage);
      }
    });
  }

  private void setupMessagesToolbar() {
    refreshButton.setOnAction(new EventHandler<ActionEvent>() {
      @Override
      public void handle(ActionEvent actionEvent) {
        fetchMessages(currentTopicNode);
      }
    });

    setupMessagesFilter();
  }

  private void setupMessagesFilter() {
    messagesModel = new MessagesModel();

    FilteredList<MessageModel> filteredData = new FilteredList<>(messagesModel.getMessages(), p -> true);

    filterField.textProperty().addListener((observable, oldValue, newValue) -> {
      filteredData.setPredicate(messageModel -> {
        // If filter text is empty, display all rows
        if (newValue == null || newValue.isEmpty()) {
          return true;
        }

        // Compare message key and message body of every message with filter text.
        String lowerCaseFilter = newValue.toLowerCase();

        if (messageModel.getKey().toLowerCase().contains(lowerCaseFilter)) {
          return true;
        } else if (messageModel.getMessageBody().toLowerCase().contains(lowerCaseFilter)) {
          return true;
        }
        return false; // Does not match.
      });
    });

    // 3. Wrap the FilteredList in a SortedList.
    SortedList<MessageModel> sortedData = new SortedList<>(filteredData);

    // 4. Bind the SortedList comparator to the TableView comparator.
    sortedData.comparatorProperty().bind(messagesTable.comparatorProperty());

    // 5. Add sorted (and filtered) data to the table.
    messagesTable.setItems(sortedData);
  }

  private void displayMessage(MessageModel message) {
    if (message != null) {
      messageArea.setText(message.getMessageBody());
    } else {
      messageArea.clear();
    }
  }

  private void setupTopicsTree() {
    var rootItem = new TreeItem<AbstractNode>(clusterNode);
    topicsTree.setRoot(rootItem);
    topicsTree.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<>() {
      @Override
      public void changed(ObservableValue<? extends TreeItem<AbstractNode>> observableValue,
          TreeItem<AbstractNode> oldItem, TreeItem<AbstractNode> newItem) {
        treeSelectionChanged(newItem);
      }
    });
  }

  private void setupKafka() {
    kafkaReader = new KafkaReader(cluster.getName(), cluster.getUrl());
    kafkaInstance = kafkaReader.getKafkaInstance();
    clusterNode = new ClusterNode(kafkaInstance);
  }

  private void treeSelectionChanged(TreeItem<AbstractNode> newItem) {
    if (followTreeSelection && newItem != null) {
      fetchMessages(newItem.getValue());
    }
  }

  private void fetchMessages(AbstractNode node) {
    setLoadingStatus(true);
    try {
      if (node == null) {
        node = getSelectedNode();
      }

      // when current node has changed, existing messages shouldn't be shown.
      // Else keep the existing messages until the new ones arrive.
      if (node != currentTopicNode) { // TODO use equals()?
        currentTopicNode = node;
        messagesModel.setMessages(node.getMessages());
      }

      var topicPartitions = getTopicPartitions(node);
      kafkaReader.getMessagesAsync(topicPartitions, 50, KafkaPaneController.this);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void setLoadingStatus(boolean isLoading) {
    followTreeSelection = !isLoading;
    messagesTable.requestFocus();
    refreshButton.setDisable(isLoading);
  }

  private void process(List<ConsumerRecord<String, String>> records) {
    int selectedRow = -1;
    var selectionModel = messagesTable.getSelectionModel();
    selectedRow = selectionModel.getSelectedIndex();
    messagesModel.setRecords(records);

    selectionModel.select(selectedRow);
  }

  private AbstractNode getSelectedNode() {
    var selectionModel = topicsTree.getSelectionModel();
    var selectedItem = selectionModel.getSelectedItem();
    return selectedItem.getValue();
  }

  @Override
  public void topicsUpdated(Map<String, List<PartitionInfo>> newTopics) {
    if (newTopics == null) {
      // TODO show alert
      return;
    }
    try {
      if (topics == null || !newTopics.keySet().equals(topics.keySet())) {
        Platform.runLater(new Runnable() {
          @Override
          public void run() {
            updateTopicsTree(newTopics);
          }
        });
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void messagesReceived(List<ConsumerRecord<String, String>> records) {
    Platform.runLater(new Runnable() {
      @Override
      public void run() {
        process(records);
        messagesTable.requestFocus();
        setLoadingStatus(false);
        var selectionModel = topicsTree.getSelectionModel();
        var selectedTreeItem = selectionModel.getSelectedItem();

        // by the time messages arrive for topic/partition, the selected
        // topic/partition might have changed. If yes, then that has to be
        // notified, because tree selection events are ignored while messages
        // are being fetched, by setting followTreeSelection=false.
        saveMessages();
        if (currentTopicNode != selectedTreeItem.getValue()) {
          treeSelectionChanged(selectedTreeItem);
        }
      }
    });
  }

  private void saveMessages() {
    var messages = new ArrayList<>(messagesModel.getMessages());
    currentTopicNode.setMessages(messages);
  }

  @Override
  public void connected(boolean really) {
    if (!really) {
      Platform.runLater(new Runnable() {
        @Override
        public void run() {
          removeClusterNode();
        }
      });
    }
  }

  private List<TopicPartition> getTopicPartitions(AbstractNode selectedNode) {
    List<TopicPartition> partitions = new ArrayList<>();

    if (selectedNode == null) {
    } else if (selectedNode instanceof TopicNode) {
      var topicNode = (TopicNode) selectedNode;
      partitions = topicNode.getTopicPartitions();
    } else if (selectedNode instanceof PartitionNode) {
      PartitionNode partitionNode = (PartitionNode) selectedNode;
      partitions.add(partitionNode.getTopicPartition());
    }

    return partitions;
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
