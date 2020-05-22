package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.MessageFormatter;
import com.loco.kafkaguru.core.KafkaReader;
import com.loco.kafkaguru.core.PluginLoader;
import com.loco.kafkaguru.core.listeners.KafkaConnectionListener;
import com.loco.kafkaguru.core.listeners.KafkaTopicsListener;
import com.loco.kafkaguru.viewmodel.AbstractNode;
import com.loco.kafkaguru.viewmodel.ClusterNode;
import com.loco.kafkaguru.viewmodel.PartitionNode;
import com.loco.kafkaguru.viewmodel.TopicNode;
import javafx.application.Platform;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.input.MouseButton;
import javafx.scene.input.MouseEvent;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.PartitionInfo;

import java.net.URL;
import java.util.*;
import java.util.prefs.Preferences;
import java.util.stream.Collectors;

@Log4j2
public class BrowseClusterViewController implements Initializable, KafkaTopicsListener, KafkaConnectionListener {
    @FXML
    private TitledPane topicsPane;
    @FXML
    private TreeView<AbstractNode> topicsTree;
    @FXML
    private CheckBox followSelectionCheck;

    private ContextMenu topicContextMenu;
    private KafkaReader kafkaReader;
    private Preferences preferences;

    private List<ClusterItemSelectionListener> treeSelectionListeners = new ArrayList<>();

    private Map<String, List<PartitionInfo>> topics;
    private ClusterNode clusterNode;
    private ControllerListener parent;
    private AbstractNode selectedNode;

    public BrowseClusterViewController(KafkaReader kafkaReader, Preferences preferences, ControllerListener parent) {
        this.kafkaReader = kafkaReader;
        this.preferences = preferences;
        this.parent = parent;
        this.clusterNode = new ClusterNode(kafkaReader.getKafkaInstance());

        kafkaReader.getKafkaInstance().addConnectionListener(this);
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        setupTopicsTree();
        setupPreferencesView();
    }

    private void setupPreferencesView() {
    }

    private void setupTopicsTree() {
        topicsTree.setShowRoot(true);
        var rootItem = new TreeItem<AbstractNode>(clusterNode);
        topicsTree.setRoot(rootItem);
        topicsTree.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<>() {
            @Override
            public void changed(ObservableValue<? extends TreeItem<AbstractNode>> observableValue,
                    TreeItem<AbstractNode> oldItem, TreeItem<AbstractNode> newItem) {
                treeSelectionChanged(newItem);
            }
        });

        topicContextMenu = new ContextMenu();
        var headerItem = new MenuItem("Select Message Format");

        topicContextMenu.getItems().add(headerItem);
        topicContextMenu.getItems().add(new SeparatorMenuItem());
        for (var name : PluginLoader.formatters.keySet()) {
            var menuItem = new RadioMenuItem(name);
            menuItem.setUserData(name);
            topicContextMenu.getItems().add(menuItem);
            menuItem.setOnAction(this::messageFormatChanged);
        }
        topicsTree.addEventHandler(MouseEvent.MOUSE_RELEASED, e -> {
            if (e.getButton() == MouseButton.SECONDARY) {
                TreeItem<AbstractNode> selected = topicsTree.getSelectionModel().getSelectedItem();

                // item is selected - this prevents fail when clicking on empty space
                if (selected != null) {
                    // open context menu on current screen position
                    openContextMenu(selected, e.getScreenX(), e.getScreenY());
                }
            } else {
                // any other click cause hiding menu
                topicContextMenu.hide();
            }
        });

        Platform.runLater(() -> {
            topicsTree.getSelectionModel().select(rootItem);
        });
    }

    private TopicNode getTopicNode(AbstractNode node) {
        TopicNode topicNode = null;
        if (node instanceof TopicNode) {
            topicNode = (TopicNode) node;
        } else if (node instanceof PartitionNode) {
            topicNode = (TopicNode) ((PartitionNode) node).getParent();
        }
        return topicNode;
    }

    private void openContextMenu(TreeItem<AbstractNode> treeItem, double x, double y) {
        // custom method that update menu items
        topicContextMenu.setUserData(treeItem.getValue());

        // show menu
        topicContextMenu.show(topicsTree, x, y);
        topicContextMenu.getItems().forEach(item -> {
            if (item instanceof RadioMenuItem) {
                var radioItem = (RadioMenuItem) item;
                radioItem.setSelected(false);
                // TODO use treeItem
                var node = getTopicNode(treeItem.getValue());
                if (item.getUserData() != null && node != null) {
                    if (radioItem.getUserData().equals(node.getFormatter().name())) {
                        radioItem.setSelected(true);
                    }
                }
            }
        });
    }

    public void topicPreferenceUpdated(ArrayList<String> nodeNames, String topic, String key, String value) {
        var topicNode = getTopicNode(topic);
        if (topicNode != null) {
            switch (key) {
                case "formatter":
                    var formatter = PluginLoader.formatters.get(value);
                    topicNode.setFormatter(formatter);
                    break;
                default:
                    break;
            }
        }
    }

    private TreeItem<AbstractNode> getLastSelectedTreeItem(TreeItem<AbstractNode> rootNode) {
        var topic = preferences.get("selected_topic", "");
        var topicNode = rootNode.getChildren().stream().filter(node -> {
            return topic.equals(((TopicNode) node.getValue()).getTopic());
        }).findFirst().orElse(null);

        if (topicNode == null)
            return null;

        var partition = preferences.getInt("selected_partition", -1);
        TreeItem<AbstractNode> partitionNode = null;

        if (partition >= 0) {
            partitionNode = topicNode.getChildren().stream()
                    .filter(node -> ((PartitionNode) node.getValue()).getPartition().partition() == partition)
                    .findFirst().orElse(null);
        }

        var selectedNode = partitionNode == null ? topicNode : partitionNode;
        return selectedNode;
    }

    private void createTopicNodes(ClusterNode clusterNode, Map<String, List<PartitionInfo>> topics) {
        var topicNodes = topics.entrySet().stream().map(entry -> createTopicNode(clusterNode, entry))
                .collect(Collectors.toList());

        clusterNode.setTopicNodes(topicNodes);
    }

    private TopicNode createTopicNode(ClusterNode clusterNode, Map.Entry<String, List<PartitionInfo>> entry) {
        var topicNode = new TopicNode(clusterNode, entry.getKey(), entry.getValue());
        // TODO remove partition node creation logic out of TopicNode class
        topicNode.setFormatter(getFormatter(topicNode.getTopic()));
        return topicNode;
    }

    private TopicNode getTopicNode(String topic) {
        var topicItem = topicsTree.getRoot().getChildren().stream().filter(item -> {
            var node = item.getValue();
            if (node instanceof TopicNode) {
                var topicName = ((TopicNode) node).getTopic();
                return topic.equals(topicName);
            }
            return false;
        }).findFirst();

        return topicItem.isEmpty() ? null : (TopicNode) topicItem.get().getValue();
    }

    private List<TreeItem<AbstractNode>> createTopicItems(ClusterNode clusterNode) {
        var topicNodes = clusterNode.getTopicNodes().stream().map(topicNode -> createTopicItem(topicNode))
                .collect(Collectors.toList());
        return topicNodes;
    }

    private TreeItem<AbstractNode> createTopicItem(TopicNode topicNode) {
        var topicItem = new TreeItem<AbstractNode>(topicNode);

        var partitionNodes = createPartitionItems(topicNode);
        topicItem.getChildren().addAll(partitionNodes);

        return topicItem;
    }

    private List<TreeItem<AbstractNode>> createPartitionItems(TopicNode topicNode) {
        return topicNode.getPartitions().stream().map(p -> new TreeItem<AbstractNode>(p)).collect(Collectors.toList());
    }

    @Override
    public void topicsUpdated(Map<String, List<PartitionInfo>> newTopics) {
        if (newTopics == null) {
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

    private void updateTopicsTree(Map<String, List<PartitionInfo>> newTopics) {
        var rootNode = topicsTree.getRoot();
        topics = newTopics;
        rootNode.getChildren().clear();

        createTopicNodes(clusterNode, topics);
        var topicItems = createTopicItems(clusterNode);
        rootNode.getChildren().addAll(topicItems);

        rootNode.setExpanded(true);

        var lastSelectedTreeItem = getLastSelectedTreeItem(rootNode);
        if (lastSelectedTreeItem != null) {
            topicsTree.getSelectionModel().select(lastSelectedTreeItem);
        }
    }

    private void treeSelectionChanged(TreeItem<AbstractNode> newItem) {
        if (newItem == null) {
            return;
        }
        selectedNode = newItem.getValue();
        treeSelectionListeners.forEach(listener -> listener.currentNodeChanged(selectedNode));
    }

    private MessageFormatter getFormatter(String topic) {
        var nodes = new ArrayList<String>();
        nodes.add("topics");
        nodes.add(topic);

        var formatterName = parent.getPreference(nodes, "formatter");
        if (!StringUtils.isEmpty(formatterName)) {
            var formatter = PluginLoader.formatters.get(formatterName);
            if (formatter != null) {
                return formatter;
            }
        }
        return PluginLoader.defaultFormatter;
    }

    private void messageFormatChanged(ActionEvent event) {
        var item = (MenuItem) event.getSource();
        var formatterName = (String) item.getUserData();
        log.info("Selected formatter " + formatterName);
        var formatter = PluginLoader.formatters.get(formatterName);

        TopicNode topicNode = getTopicNode(selectedNode);
        if (topicNode != null) {
            log.info("Selected topic " + topicNode.getTopic());
            topicNode.setFormatter(formatter);
            // messagesModel.setMessages(currentTopicNode.getMessages());
            saveFormatter(topicNode.getTopic(), formatter);
        } else {
            log.info("Selected topic is null");
        }
    }

    private void saveFormatter(String topic, MessageFormatter formatter) {
        var nodes = new ArrayList<String>();
        nodes.add("topics");
        nodes.add(topic);

        parent.savePreference(nodes, "formatter", formatter.name());
    }

    public void addItemSelectionListener(ClusterItemSelectionListener listener) {
        treeSelectionListeners.add(listener);
    }

    public void removeItemSelectionListener(ClusterItemSelectionListener listener) {
        treeSelectionListeners.remove(listener);
    }

    @Override
    public void connected(boolean really) {

    }

    @Override
    public void notifyUrlChange(String name, String oldUrl, String newUrl) {
    }

    @Override
    public void notifyNameChange(String id, String oldName, String newName) {
        topicsTree.getRoot().setValue(clusterNode);
    }
}
