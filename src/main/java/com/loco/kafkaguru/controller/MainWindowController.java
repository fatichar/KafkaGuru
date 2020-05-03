package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.model.KafkaClusterInfo;
import com.loco.kafkaguru.view.KafkaPane;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

import java.net.URL;
import java.util.*;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;
import java.util.stream.Collectors;

@Getter
@Setter
@Log4j2
public class MainWindowController implements Initializable, ControllerListener {
    @FXML
    private MenuItem newClusterMenuItem;
    @FXML
    private Menu openClusterMenu;
    @FXML
    private MenuItem closeClusterMenuItem;
    @FXML
    private Menu removeClusterMenu;

    @FXML
    private TabPane tabPane;

    private Preferences preferences;
    private Map<String, KafkaPaneController> controllers = new HashMap<>();
    // private Map<String, Tab> tabs = new HashMap<>();
    private Map<String, KafkaClusterInfo> clusters = new HashMap<>();

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        preferences = readPreferences();
        clusters = createClusters(preferences);
        createMenuItems(clusters);
        controllers = createControllers(preferences, clusters);
        var tabs = controllers.values().stream().map(this::createTab).collect(Collectors.toList());
        tabPane.getTabs().addAll(tabs);
    }

    /**
     * Read preferences from file.
     *
     * @return Preferences object for locally saved preferences
     */
    private Preferences readPreferences() {
        return Preferences.userRoot().node(this.getClass().getName());
    }

    private Map<String, KafkaClusterInfo> createClusters(Preferences appPreferences) {
        try {
            var clustersNode = appPreferences.node("clusters");
            var clusterNames = clustersNode.childrenNames();
            var clusters = Arrays.stream(clusterNames) //
                    .map(clusterName -> clustersNode.node(clusterName)) //
                    .map(this::createCluster)
                    .collect(Collectors.toMap((cluster -> cluster.getName()), (cluster -> cluster)));

            return clusters;
        } catch (BackingStoreException e) {
            e.printStackTrace();
        }
        return null;
    }

    private KafkaClusterInfo createCluster(Preferences preferences) {
        var clusterName = preferences.name();
        var url = preferences.get("url", null);

        return new KafkaClusterInfo(clusterName, url);
    }

    private Map<String, KafkaPaneController> createControllers(Preferences preferences,
            Map<String, KafkaClusterInfo> clusters) {
        var controllers = new TreeMap<String, KafkaPaneController>();
        List<Preferences> tabNodes = null;
        try {
            var tabsNode = preferences.node("tabs");
            var tabIds = tabsNode.childrenNames();
            tabNodes = Arrays.stream(tabIds) //
                    .map(tabId -> tabsNode.node(tabId)) //
                    .collect(Collectors.toList());
        } catch (BackingStoreException e) {
            e.printStackTrace();
            return controllers;
        }

        for (var tabNode : tabNodes) {
            var clusterName = getclusterName(tabNode);
            var cluster = clusters.get(clusterName);
            if (cluster == null) {
                log.error(
                        "Tab with name {} could not be loaded, because "
                                + "the cluster {} could not be found in saved preferences",
                        tabNode.name(), clusterName);
                continue;
            }
            var controllerId = tabNode.name();
            if (controllerId == null) {
                log.error("Too many tabs for cluster {}", clusterName);
                continue;
            }
            var controller = new KafkaPaneController(cluster, this, tabNode);
            controllers.put(controllerId, controller);
        }

        return controllers;
    }

    private String createControllerId(KafkaClusterInfo cluster) {
        var clusterName = cluster.getName();
        var controllerId = clusterName;
        byte i = 2;

        do {
            if (!controllers.containsKey(controllerId)) {
                return controllerId;
            }
            controllerId = clusterName + "(" + i + ")";
        } while (++i > 0);

        return null;
    }

    private String getclusterName(Preferences preferences) {
        return preferences.get("cluster_id", null);
    }

    private Tab createTab(KafkaPaneController controller) {
        KafkaPane kafkaPane = new KafkaPane(controller);

        Tab tab = new Tab(controller.getId());
        tab.setId(controller.getId());
        tab.setContent(kafkaPane);

        return tab;
    }

    private void createMenuItems(Map<String, KafkaClusterInfo> clusters) {
        clusters.keySet().forEach(clusterName -> {
            var cluster = clusters.get(clusterName);

            createOpenClusterMenuItem(cluster);

            createRemoveClusterMenuItem(cluster);
        });
    }

    private void createOpenClusterMenuItem(KafkaClusterInfo cluster) {
        var openClusterItem = new MenuItem(cluster.getName());
        openClusterItem.setId(cluster.getName());
        openClusterItem.setOnAction(this::onOpenCluster);
        openClusterMenu.getItems().add(openClusterItem);
    }

    private void createRemoveClusterMenuItem(KafkaClusterInfo cluster) {
        var removeClusterItem = new MenuItem(cluster.getName());
        removeClusterItem.setId(cluster.getName());
        removeClusterItem.setOnAction(this::onRemoveCluster);
        removeClusterMenu.getItems().add(removeClusterItem);
    }

    public void onNewCluster(ActionEvent actionEvent) {
        KafkaClusterInfo cluster = getNewClusterInfo();
        if (cluster == null) {
            return;
        }
        clusters.put(cluster.getName(), cluster);
        save(cluster);

        createOpenClusterMenuItem(cluster);
        createRemoveClusterMenuItem(cluster);

        var controller = createController(cluster);
        controllers.put(controller.getId(), controller);

        Tab newTab = createTab(controller);
        tabPane.getTabs().add(newTab);
        tabPane.getSelectionModel().select(newTab);
    }

    private Preferences save(KafkaClusterInfo cluster) {
        var childPreferences = preferences.node("clusters").node(cluster.getName());
        childPreferences.put("url", cluster.getUrl());
        savePreferences(childPreferences);
        return childPreferences;
    }

    public void onOpenCluster(ActionEvent event) {
        var menuItem = (MenuItem) event.getSource();

        var clusterName = menuItem.getId();
        var cluster = clusters.get(clusterName);

        var controller = createController(cluster);
        controllers.put(controller.getId(), controller);

        var tab = createTab(controller);
        tabPane.getTabs().add(tab);
    }

    private KafkaPaneController createController(KafkaClusterInfo cluster) {
        var controllerId = createControllerId(cluster);
        var tabPreferences = preferences.node("tabs").node(controllerId);
        tabPreferences.put("cluster_id", cluster.getName());
        var controller = new KafkaPaneController(cluster, this, tabPreferences);
        return controller;
    }

    public void onRemoveCluster(ActionEvent event) {
        boolean removeTabs = getConfirmation("Removing the cluster will close associated tabs. Proceed?");
        if (!removeTabs) {
            return;
        }
        var menuItem = (MenuItem) event.getSource();
        var clusterName = (String) menuItem.getId();
        clusters.remove(clusterName);
        removeClusterMenu.getItems().remove(menuItem);
        openClusterMenu.getItems().removeIf(item -> item.getId().equals(clusterName));

        removeTabs(clusterName);
        try {
            var node = preferences.node("clusters").node(clusterName);
            node.removeNode();
            node.flush();
        } catch (BackingStoreException e) {
            e.printStackTrace();
        }
    }

    private void removeTabs(String removedClusterId) {
        try {
            var tabsNode = preferences.node("tabs");
            var tabNames = tabsNode.childrenNames();
            for (String tabName : tabNames) {
                var node = tabsNode.node(tabName);
                var clusterId = node.get("cluster_id", "");
                if (removedClusterId.equals(clusterId)) {
                    node.removeNode();
                    controllers.remove(tabName);
                    tabPane.getTabs().removeIf(tab -> tab.getId().equals(tabName));
                }
            }
            preferences.flush();
        } catch (BackingStoreException e) {
            e.printStackTrace();
        }
    }

    public void onCloseTab(ActionEvent actionEvent) {
        var activeTab = tabPane.getSelectionModel().getSelectedItem();
        if (activeTab == null) {
            return;
        }

        tabPane.getTabs().remove(activeTab);

        boolean keepSaved = getConfirmation("Do you want to see this tab on next launch?");

        if (keepSaved) {
            return;
        }

        try {
            var node = preferences.node("tabs").node(activeTab.getId());
            node.removeNode();
            node.flush();
        } catch (BackingStoreException e) {
            e.printStackTrace();
        }
    }

    private boolean getConfirmation(String msg) {
        return new Alert(Alert.AlertType.CONFIRMATION, msg, ButtonType.YES, ButtonType.NO).showAndWait()
                .orElse(ButtonType.NO).equals(ButtonType.YES);
    }

    private KafkaClusterInfo getNewClusterInfo() {
        String kafkaUrl = getUserInput("Enter kafka URL");
        if (kafkaUrl == null)
            return null;
        if (kafkaUrl.isEmpty()) {
            return null; // TODO show alert
        }

        var clusterName = getUserInput("Give a friendly name to this kafka instance");
        if (clusterName == null)
            return null;
        if (clusterName.isEmpty()) {
            return null; // TODO show alert
        }

        return new KafkaClusterInfo(clusterName, kafkaUrl);
    }

    private String getUserInput(String message) {
        var inputDialog = new TextInputDialog();
        inputDialog.setHeaderText(message);
        var result = inputDialog.showAndWait();
        if (result.isEmpty()) {
            return null;
        }

        var kafkaUrl = result.get();
        return kafkaUrl;
    }

    @Override
    public void destroy(KafkaPaneController controller) {
        tabPane.getTabs().remove(controller.getId());
        controllers.remove(controller);
    }

    /**
     * Saves given preferences to file.
     *
     * @param preferences
     */
    private void savePreferences(Preferences preferences) {
        try {
            preferences.flush();
        } catch (BackingStoreException e) {
            e.printStackTrace();
            new Alert(Alert.AlertType.ERROR, "Failed to save preferences").showAndWait();
        }
    }
}
