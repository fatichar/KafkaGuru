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
public class MainWindowController implements Initializable {
    @FXML private Button newTabButton;

    @FXML private TabPane tabPane;

    private Preferences preferences;
    private List<KafkaPaneController> childControllers;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        preferences = readPreferences();
        setPreferences(preferences);
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

    /** Sets current preferences for the application. */
    private void setPreferences(Preferences preferences) {
        try {
            var childrenNames = preferences.childrenNames();
//            childControllers = new ArrayList<>();
            childControllers =
                    Arrays.stream(childrenNames)
                            .map(createChild(preferences))
                            .collect(Collectors.toUnmodifiableList());
        } catch (BackingStoreException e) {
            // TODO handle error
        }
    }

    private Function<String, KafkaPaneController> createChild(Preferences preferences) {
        return childName -> {
            var childNode = preferences.node(childName);
            return new KafkaPaneController(childNode);
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
            new javafx.scene.control.Alert(Alert.AlertType.ERROR, "Failed to save preferences");
        }
    }

    private void updateUI() {
        childControllers.stream().forEach(this::createTab);
    }

    public void onNewConnection(ActionEvent actionEvent) {
        KafkaClusterInfo cluster = getNewClusterInfo();
        if (cluster == null){
            return;
        }
        var childPreferences = preferences.node(cluster.getName());
        childPreferences.put("name", cluster.getName());
        childPreferences.put("url", cluster.getUrl());

        var controller = new KafkaPaneController(childPreferences);
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
        //        inputDialog.setHeaderText("Enter kafka URL");
        //        inputDialog.showAndWait();
        //
        //        var kafkaUrl = inputDialog.getEditor().getText();
        //        inputDialog.setHeaderText("Give a friendly name to this kafka instance");
        //        inputDialog.getEditor().clear();
        //        inputDialog.showAndWait();
        //        var kafkaName = inputDialog.getEditor().getText();

        //        return new KafkaClusterInfo(kafkaName, kafkaUrl);
        return new KafkaClusterInfo(
                "Dev Sandbox", "ec2-54-226-137-43.compute-1.amazonaws.com:9092");
    }
}
