package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.model.KafkaClusterInfo;
import com.loco.kafkaguru.view.KafkaPane;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import lombok.Getter;
import lombok.Setter;

import java.net.URL;
import java.util.*;
import java.util.function.Function;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;
import java.util.stream.Collectors;

@Getter
@Setter
public class MainWindowController implements Initializable, ControllerListener {
    @FXML
    private Button newTabButton;

    @FXML
    private TabPane tabPane;

    private Preferences preferences;
    private Map<KafkaPaneController, Tab> childControllers = new HashMap<>();

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        preferences = readPreferences();
        updateUI();
    }

    /**
     * Read preferences from file.
     *
     * @return Preferences object for locally saved preferences
     */
    private Preferences readPreferences() {
        return Preferences.userRoot().node(this.getClass().getName());
    }

    private Function<String, KafkaPaneController> createChild(Preferences preferences) {
        return childName -> {
            var childNode = preferences.node(childName);
            return new KafkaPaneController((ControllerListener) this, childNode);
        };
    }

    /**
     * Saves given preferences to file.
     *
     * @param preferences
     */
    private void savePreferences(Preferences preferences) {
        preferences.putInt("selected_tab_index", tabPane.getSelectionModel().getSelectedIndex());
        try {
            preferences.flush();
        } catch (BackingStoreException e) {
            e.printStackTrace();
            new javafx.scene.control.Alert(Alert.AlertType.ERROR, "Failed to save preferences").showAndWait();
        }
    }

    private void updateUI() {
        try {
            var childrenNames = preferences.childrenNames();

            var controllers = Arrays.stream(childrenNames).map(createChild(preferences))
                    .collect(Collectors.toUnmodifiableList());

            controllers.forEach(controller -> childControllers.put(controller, createTab(controller)));
        } catch (BackingStoreException e) {
            // TODO handle error
        }
    }

    public void onNewConnection(ActionEvent actionEvent) {
        KafkaClusterInfo cluster = getNewClusterInfo();
        if (cluster == null) {
            return;
        }
        var childPreferences = preferences.node(cluster.getName());
        childPreferences.put("name", cluster.getName());
        childPreferences.put("url", cluster.getUrl());

        var controller = new KafkaPaneController(this, childPreferences);
        Tab newTab = createTab(controller);
        tabPane.getSelectionModel().select(newTab);

        savePreferences(preferences);
    }

    private Tab createTab(KafkaPaneController controller) {
        KafkaPane kafkaPane = new KafkaPane(controller);

        Tab tab = new Tab(controller.getName());
        tab.setContent(kafkaPane);

        tabPane.getTabs().add(tab);
        return tab;
    }

    private KafkaClusterInfo getNewClusterInfo() {
        var inputDialog = new TextInputDialog();
        inputDialog.setHeaderText("Enter kafka URL");
        inputDialog.showAndWait();

        var kafkaUrl = inputDialog.getEditor().getText();
        inputDialog.setHeaderText("Give a friendly name to this kafka instance");
        inputDialog.getEditor().clear();
        inputDialog.showAndWait();
        var kafkaName = inputDialog.getEditor().getText();

        return new KafkaClusterInfo(kafkaName, kafkaUrl);
    }

    @Override
    public void destroy(KafkaPaneController controller) {
        var tab = childControllers.getOrDefault(controller, null);
        if (tab != null) {
            tabPane.getTabs().remove(tab);
            childControllers.remove(controller);
        }
    }
}
