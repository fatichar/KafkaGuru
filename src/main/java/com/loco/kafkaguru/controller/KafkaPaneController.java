package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.core.KafkaInstance;
import com.loco.kafkaguru.core.KafkaReader;
import com.loco.kafkaguru.core.listeners.KafkaListener;
import com.loco.kafkaguru.model.KafkaClusterInfo;
import com.loco.kafkaguru.viewmodel.*;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleObjectProperty;
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
import java.util.function.Predicate;
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
    private CheckBox followSelectionCheck;
    @FXML
    private ComboBox<String> messageCountBox;

    // messages toolbar
    @FXML
    private Button refreshButton;
    @FXML
    private TextField includeField;
    @FXML
    private TextField excludeField;

    // messages table
    @FXML
    private TableView<MessageModel> messagesTable;
    @FXML
    private TableColumn<MessageModel, Integer> rowNumberColumn;
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

    private ControllerListener parent;
    private Preferences preferences;
    private BooleanProperty followTreeSelection = new SimpleBooleanProperty(true);

    // This is the node which is currently associated with the messages table.
    private AbstractNode currentTopicNode;
    boolean currentNodeStale = false;
    private boolean loading = false;
    private int maxMessagesToFetch = 50;
    private boolean connected;

    public KafkaPaneController(ControllerListener parent, Preferences preferences) {
        this.parent = parent;
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
        setupPreferencesView();

        // TODO report connection error
        kafkaInstance.connectAsync(this);
    }

    private void setupPreferencesView() {
        followSelectionCheck.selectedProperty().bindBidirectional(followTreeSelection);
        messageCountBox.setValue("" + maxMessagesToFetch);
        messageCountBox.valueProperty().addListener(new ChangeListener<String>() {
            @Override
            public void changed(ObservableValue<? extends String> observableValue, String oldValue, String newValue) {
                try {
                    maxMessagesToFetch = Integer.parseInt(newValue);
                } catch (NumberFormatException e) {
                    maxMessagesToFetch = 50;
                }
            }
        });
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

    private void setupMessagesView() {
        setupMessagesToolbar();

        rowNumberColumn.setCellValueFactory(new PropertyValueFactory<>("index"));
        partitionColumn.setCellValueFactory(new PropertyValueFactory<>("partition"));
        offsetColumn.setCellValueFactory(new PropertyValueFactory<>("offset"));
        keyColumn.setCellValueFactory(new PropertyValueFactory<>("key"));
        messageSummaryColumn.setCellValueFactory(new PropertyValueFactory<>("messageSummary"));
        timestampColumn.setCellValueFactory(new PropertyValueFactory<>("timestamp"));

        messagesTable.getSelectionModel().selectedItemProperty()
                .addListener((observableValue, oldMessage, newMessage) -> displayMessage(newMessage));
    }

    private void setupMessagesToolbar() {
        refreshButton.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent actionEvent) {
                messagesTable.requestFocus();
                refreshMessages();
            }
        });

        setupMessagesFilter();
    }

    private void refreshMessages() {
        currentTopicNode = topicsTree.getSelectionModel().getSelectedItem().getValue();
        fetchMessages(currentTopicNode);
    }

    private void setupMessagesFilter() {
        messagesModel = new MessagesModel();

        ObjectProperty<Predicate<MessageModel>> includeFilter = new SimpleObjectProperty<>();
        ObjectProperty<Predicate<MessageModel>> excludeFilter = new SimpleObjectProperty<>();

        includeFilter.bind(Bindings.createObjectBinding(() -> message -> {
            var filter = includeField.getText().toLowerCase();
            var body = message.getMessageBody().toLowerCase();
            return StringUtils.isEmpty(filter) || body.contains(filter);
        }, includeField.textProperty()));

        excludeFilter.bind(Bindings.createObjectBinding(() -> message -> {
            var filter = excludeField.getText().toLowerCase();
            var body = message.getMessageBody().toLowerCase();
            return StringUtils.isEmpty(filter) || !body.contains(filter);
        }, excludeField.textProperty()));

        FilteredList<MessageModel> filteredData = new FilteredList<>(messagesModel.getMessages());

        filteredData.predicateProperty().bind(Bindings
                .createObjectBinding(() -> includeFilter.get().and(excludeFilter.get()), includeFilter, excludeFilter));

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
        if (!connected) {
            return;
        }
        if (newItem == null) {
            return;
        }

        if (followTreeSelection.get()) {
            var selectedNode = newItem.getValue();

            saveSelectionPreference(newItem);

            messagesModel.setMessages(selectedNode.getMessages());
            currentTopicNode = selectedNode;
            if (loading) {
                currentNodeStale = true;
            } else {
                fetchMessages(selectedNode);
            }
        }
    }

    private void saveSelectionPreference(TreeItem<AbstractNode> selectedTreeItem) {
        AbstractNode selectedNode = selectedTreeItem.getValue();

        var selectedTopic = "";
        var selectedPartition = -1;
        if (selectedNode instanceof TopicNode) {
            selectedTopic = ((TopicNode) selectedNode).getTopic();
        } else {
            var partitionNode = (PartitionNode) selectedNode;
            selectedPartition = partitionNode.getPartition().partition();
            selectedTopic = partitionNode.getPartition().topic();
        }

        preferences.put("selectedTopic", selectedTopic);
        preferences.putInt("selectedPartition", selectedPartition);
    }

    private void fetchMessages(AbstractNode node) {
        setLoadingStatus(true);
        try {
            var topicPartitions = getTopicPartitions(node);
            kafkaReader.getMessagesAsync(topicPartitions, maxMessagesToFetch, KafkaPaneController.this, node);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setLoadingStatus(boolean isLoading) {
        this.loading = isLoading;
        refreshButton.setDisable(isLoading);
    }

    private static List<MessageModel> createMessages(List<ConsumerRecord<String, String>> records) {
        final var row = new Object() {
            public int value = 0;
        };
        var messages = records.stream().map(record -> new MessageModel(++row.value, record))
                .collect(Collectors.toList());
        return messages;
    }

    @Override
    public void topicsUpdated(Map<String, List<PartitionInfo>> newTopics) {
        if (newTopics == null) {
            connected(false);
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
    public void messagesReceived(List<ConsumerRecord<String, String>> records, Object sender) {
        Platform.runLater(new Runnable() {
            @Override
            public void run() {
                // update the sender node
                var senderNode = (AbstractNode) sender;
                var messages = createMessages(records);
                senderNode.setMessages(messages);

                setLoadingStatus(false);

                if (currentTopicNode == senderNode) {
                    updateMessagesTable(messages);
                } else {
                    if (currentNodeStale) {
                        fetchMessages(currentTopicNode);
                        currentNodeStale = false;
                    } else {
                        new Alert(Alert.AlertType.WARNING,
                                "currentTopicNode != senderNode, and currentNodeStale is false").showAndWait();
                    }
                }
            }
        });
    }

    private void updateMessagesTable(List<MessageModel> messages) {
        var selectionModel = messagesTable.getSelectionModel();
        int selectedRow = selectionModel.getSelectedIndex();
        messagesModel.setMessages(messages);
        selectionModel.select(selectedRow);
        messagesTable.requestFocus();
    }

    @Override
    public void connected(boolean really) {
        if (really) {
            this.connected = true;
        } else {
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

        var rootNode = topicsTree.getRoot();
        topics = newTopics;
        rootNode.getChildren().clear();

        var topicNodes = createTopicNodes(clusterNode, topics);
        rootNode.getChildren().addAll(topicNodes);

        rootNode.setExpanded(true);

        var lastSelectedTreeItem = getLastSelectedTreeItem(rootNode);
        if (lastSelectedTreeItem != null) {
            topicsTree.getSelectionModel().select(lastSelectedTreeItem);
        }
    }

    private TreeItem<AbstractNode> getLastSelectedTreeItem(TreeItem<AbstractNode> rootNode) {
        var topic = preferences.get("selectedTopic", "");
        var topicNode = rootNode.getChildren().stream().filter(node -> {
            return topic.equals(((TopicNode) node.getValue()).getTopic());
        }).findFirst().orElse(null);

        if (topicNode == null)
            return null;

        var partition = preferences.getInt("selectedPartition", -1);
        TreeItem<AbstractNode> partitionNode = null;

        if (partition >= 0) {
            partitionNode = topicNode.getChildren().stream()
                    .filter(node -> ((PartitionNode) node.getValue()).getPartition().partition() == partition)
                    .findFirst().orElse(null);
        }

        var selectedNode = partitionNode == null ? topicNode : partitionNode;
        return selectedNode;
    }

    private List<TreeItem<AbstractNode>> createTopicNodes(ClusterNode clusterNode,
            Map<String, List<PartitionInfo>> topics) {
        var topicNodes = topics.entrySet().stream()
                .map(entry -> createTopicNode(clusterNode, entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
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
