package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.MessageFormatter;
import com.loco.kafkaguru.core.KafkaReader;
import com.loco.kafkaguru.core.PluginLoader;
import com.loco.kafkaguru.core.listeners.KafkaConnectionListener;
import com.loco.kafkaguru.core.listeners.KafkaMessagesListener;
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
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import tornadofx.control.DateTimePicker;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Log4j2
public class BrowseClusterItemViewController
        implements Initializable,
                ClusterItemSelectionListener,
                KafkaMessagesListener,
                KafkaConnectionListener {
    private static final String SAVE_MESSAGE_DIR = "Saved Messages";

    @FXML private VBox mainLayout;
    @FXML private AnchorPane clusterDetailsPane;
    @FXML private HBox messagesBox;
    @FXML private TitledPane settingsPane;
    @FXML private GridPane settingsGrid;

    @FXML private TextField clusterNameField;
    @FXML private TextField kafkaUrlField;
    @FXML private Button connectButton;

    // messages toolbar
    @FXML private Button refreshButton;
    @FXML private TextField includeField;
    @FXML private TextField excludeField;
    @FXML private ComboBox<String> messageCountBox;
    @FXML private ComboBox<String> cursorBox;
    @FXML Button collapseSettingsButton;

    // messages table
    @FXML private TableView<MessageModel> messagesTable;
    @FXML private TableColumn<MessageModel, Integer> rowNumberColumn;
    @FXML private TableColumn<MessageModel, Integer> partitionColumn;
    @FXML private TableColumn<MessageModel, Long> offsetColumn;
    @FXML private TableColumn<MessageModel, String> keyColumn;
    @FXML private TableColumn<MessageModel, String> messageSummaryColumn;
    @FXML private TableColumn<MessageModel, Date> timestampColumn;
    @FXML private TextArea messageArea;

    private CusterItemViewSettings settings;

    private MessagesModel messagesModel;
    private boolean loading = false;
    private long fetchFrom = -1;
    boolean currentNodeStale = false;
    private KafkaReader kafkaReader;
    private AbstractNode currentNode;
    private AbstractNode selectedNode;
    private BooleanProperty followTreeSelection = new SimpleBooleanProperty(true);

    public BrowseClusterItemViewController(
            KafkaReader kafkaReader, CusterItemViewSettings settings) {
        this.kafkaReader = kafkaReader;
        this.settings = settings;
        kafkaReader.getKafkaInstance().addConnectionListener(this);
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        setupClusterView();
        setupMessagesView();
    }

    private void setupClusterView() {
        clusterNameField.setText(kafkaReader.getKafkaInstance().getName());
        kafkaUrlField.setText(kafkaReader.getKafkaInstance().getUrl());
        connectButton.setOnAction(this::onConnectButtonClick);
    }

    private void setupMessagesView() {
        setupMessagesToolbar();
        setupSettingsPane();

        rowNumberColumn.setCellValueFactory(new PropertyValueFactory<>("index"));
        partitionColumn.setCellValueFactory(new PropertyValueFactory<>("partition"));
        offsetColumn.setCellValueFactory(new PropertyValueFactory<>("offset"));
        keyColumn.setCellValueFactory(new PropertyValueFactory<>("key"));
        messageSummaryColumn.setCellValueFactory(new PropertyValueFactory<>("messageSummary"));
        timestampColumn.setCellValueFactory(new PropertyValueFactory<>("timestamp"));

        messagesTable
                .getSelectionModel()
                .selectedItemProperty()
                .addListener(
                        (observableValue, oldMessage, newMessage) -> displayMessage(newMessage));

        var messagesContextMenu = new ContextMenu();
        var saveItem = new MenuItem("Save selected message");
        var saveAllItem = new MenuItem("Save all messages");
        final String format = ".dat";
        saveItem.setOnAction(
                event -> {
                    var messages = getSelectedMessages();
                    save(messages, format);
                });
        saveItem.setOnAction(
                event -> {
                    var messages = messagesTable.getItems();
                    save(messages, format);
                });
        messagesContextMenu.getItems().add(saveItem);
        messagesTable.setContextMenu(messagesContextMenu);
    }

    private void setupSettingsPane() {
        var dateTimePicker = new DateTimePicker();
        dateTimePicker.dateTimeValueProperty().setValue(LocalDateTime.now());
        settingsGrid.add(dateTimePicker, 1, 4, 2, 1);
    }

    private void setupCursorBox() {
        this.cursorBox.setValue("End");
        cursorBox
                .valueProperty()
                .addListener(
                        new ChangeListener<String>() {
                            @Override
                            public void changed(
                                    ObservableValue<? extends String> observableValue,
                                    String oldValue,
                                    String newValue) {
                                switch (newValue) {
                                    case "Beginning":
                                        fetchFrom = 0;
                                        break;
                                    case "End":
                                        fetchFrom = -1;
                                        break;
                                    default:
                                        try {
                                            fetchFrom = Integer.parseInt(newValue);
                                        } catch (NumberFormatException e) {
                                            cursorBox.valueProperty().set(oldValue);
                                        }
                                        break;
                                }
                            }
                        });
    }

    private void setupMessageCountBox() {
        messageCountBox.setValue("" + settings.getBatchSize());
        messageCountBox
                .valueProperty()
                .addListener(
                        new ChangeListener<String>() {
                            @Override
                            public void changed(
                                    ObservableValue<? extends String> observableValue,
                                    String oldValue,
                                    String newValue) {
                                try {
                                    settings.setBatchSize(Integer.parseInt(newValue));
                                } catch (NumberFormatException e) {
                                    settings.setBatchSize(Integer.parseInt(oldValue));
                                }
                            }
                        });
    }

    private void onConnectButtonClick(ActionEvent actionEvent) {
        if (isNameChanged()) {
            kafkaReader.getKafkaInstance().setName(clusterNameField.getText());
        }
        if (isUrlChanged()) {
            kafkaReader.getKafkaInstance().setUrl(kafkaUrlField.getText());
        }
        kafkaReader.getKafkaInstance().connectAsync();
    }

    private boolean isNameChanged() {
        var old = kafkaReader.getKafkaInstance().getName();
        var current = clusterNameField.getText();
        return !StringUtils.equals(old, current);
    }

    private boolean isUrlChanged() {
        var old = kafkaReader.getKafkaInstance().getUrl();
        var current = kafkaUrlField.getText();
        return !StringUtils.equals(old, current);
    }

    @Override
    public void connected(String id, boolean really) {
        connectButton.setDisable(really);
    }

    @Override
    public void notifyUrlChange(String name, String oldUrl, String newUrl) {
        kafkaUrlField.setText(newUrl);
    }

    @Override
    public void notifyNameChange(String id, String oldName, String newName) {
        clusterNameField.setText(newName);
    }

    @Override
    public void currentNodeChanged(AbstractNode selectedNode) {
        if (selectedNode == null) {
            return;
        }

        this.selectedNode = selectedNode;

        if (followTreeSelection.get()) {
            updateView(selectedNode);
        }
    }

    private void updateView(AbstractNode selectedNode) {
        switch (selectedNode.getType()) {
            case CLUSTER:
                mainLayout.getChildren().clear();
                mainLayout.getChildren().add(clusterDetailsPane);
                clusterDetailsPane.setVisible(true);
                break;
            case TOPIC:
            case PARTITION:
                mainLayout.getChildren().clear();
                mainLayout.getChildren().add(messagesBox);
                messagesBox.setVisible(true);
                messagesModel.setMessages(selectedNode.getMessages());
                currentNode = selectedNode;
                if (loading) {
                    currentNodeStale = true;
                } else {
                    fetchMessages(selectedNode);
                }
        }
    }

    @Override
    public void messagesReceived(
            List<ConsumerRecord<String, byte[]>> records,
            Object sender,
            int batchNumber,
            boolean moreToCome) {
        log.info("Received {} messages", records.size());
        Platform.runLater(
                () -> {
                    log.info("Processing {} messages", records.size());
                    // update the sender node
                    var senderNode = (AbstractNode) sender;
                    var formatter = getFormatter(senderNode);
                    if (batchNumber == 1) {
                        var messages = createMessages(0, records, formatter);
                        senderNode.setMessages(messages);
                    } else {
                        var messages =
                                createMessages(senderNode.getMessages().size(), records, formatter);
                        senderNode.addMessages(messages);
                    }
                    log.info("Added {} messages to the node", records.size());

                    setLoadingStatus(moreToCome);

                    if (currentNode == senderNode) {
                        updateMessagesTable();
                        log.info("Added {} messages to the table", records.size());
                    } else {
                        if (currentNodeStale) {
                            fetchMessages(currentNode);
                            currentNodeStale = false;
                        } else {
                            new Alert(
                                            Alert.AlertType.WARNING,
                                            "currentTopicNode != senderNode, and currentNodeStale is false")
                                    .showAndWait();
                        }
                    }
                });
    }

    private MessageFormatter getFormatter(AbstractNode senderNode) {
        var formatter = PluginLoader.defaultFormatter;
        var topicNode = getTopicNode(senderNode);
        if (topicNode != null) {
            formatter = topicNode.getFormatter();
        }
        return formatter;
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

    @Override
    public void messageFormatChanged(String topic) {
        updateMessagesTable();
    }

    private void updateMessagesTable() {
        var selectionModel = messagesTable.getSelectionModel();
        int selectedRow = selectionModel.getSelectedIndex();
        messagesModel.setMessages(currentNode.getMessages());
        selectionModel.select(selectedRow);
        messagesTable.requestFocus();
    }

    private void fetchMessages(AbstractNode node) {
        setLoadingStatus(true);
        try {
            var topicPartitions = getTopicPartitions(node);
            kafkaReader.getMessagesAsync(
                    topicPartitions, settings.getBatchSize(), fetchFrom, this, node);
        } catch (Exception e) {
            e.printStackTrace();
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

    private void setLoadingStatus(boolean isLoading) {
        this.loading = isLoading;
        refreshButton.setDisable(isLoading);
    }

    private static List<MessageModel> createMessages(
            int startRow,
            List<ConsumerRecord<String, byte[]>> records,
            MessageFormatter formatter) {
        final var row =
                new Object() {
                    public int value = startRow;
                };

        var messages =
                records.stream()
                        .map(record -> new MessageModel(++row.value, record, formatter))
                        .collect(Collectors.toList());
        return messages;
    }

    private void save(List<MessageModel> messages, String format) {
        messages.forEach(message -> save(message, format));
    }

    private void save(MessageModel message, String format) {
        var path =
                Paths.get(
                        SAVE_MESSAGE_DIR,
                        kafkaReader.getKafkaInstance().getName(),
                        message.getRecord().topic(),
                        "" + message.getPartition(),
                        "" + message.getOffset() + format);

        try {
            File file = new File(path.toUri());
            file.getParentFile().mkdirs();
            Files.write(path, message.getRecord().value());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<MessageModel> getSelectedMessages() {
        return messagesTable.getSelectionModel().getSelectedItems();
    }

    private void setupMessagesToolbar() {
        refreshButton.setOnAction(
                actionEvent -> {
                    messagesTable.requestFocus();
                    refreshMessages();
                });

        setupMessageCountBox();
        setupCursorBox();
        setupMessagesFilter();
        collapseSettingsButton.setOnAction(
                actionEvent -> {
                    if (messagesBox.getChildren().contains(settingsPane)){
                        messagesBox.getChildren().remove(settingsPane);
                        collapseSettingsButton.setText("<<");
                    } else {
                        messagesBox.getChildren().add(settingsPane);
                        collapseSettingsButton.setText(">>");
                    }
                });
    }

    private void refreshMessages() {
        currentNode = selectedNode;
        fetchMessages(currentNode);
    }

    private void setupMessagesFilter() {
        messagesModel = new MessagesModel();

        ObjectProperty<Predicate<MessageModel>> includeFilter = new SimpleObjectProperty<>();
        ObjectProperty<Predicate<MessageModel>> excludeFilter = new SimpleObjectProperty<>();

        includeFilter.bind(
                Bindings.createObjectBinding(
                        () ->
                                message -> {
                                    var filter = includeField.getText().toLowerCase();
                                    var body = message.getMessageBody().toLowerCase();
                                    return StringUtils.isEmpty(filter) || body.contains(filter);
                                },
                        includeField.textProperty()));

        excludeFilter.bind(
                Bindings.createObjectBinding(
                        () ->
                                message -> {
                                    var filter = excludeField.getText().toLowerCase();
                                    var body = message.getMessageBody().toLowerCase();
                                    return StringUtils.isEmpty(filter) || !body.contains(filter);
                                },
                        excludeField.textProperty()));

        FilteredList<MessageModel> filteredData = new FilteredList<>(messagesModel.getMessages());

        filteredData
                .predicateProperty()
                .bind(
                        Bindings.createObjectBinding(
                                () -> includeFilter.get().and(excludeFilter.get()),
                                includeFilter,
                                excludeFilter));

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

    public void topicPreferenceUpdated(
            ArrayList<String> nodeNames, String topic, String key, String value) {
        messagesModel.setMessages(currentNode.getMessages());
    }
}
