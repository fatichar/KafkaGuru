package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.core.KafkaInstance;
import com.loco.kafkaguru.core.PluginLoader;
import com.loco.kafkaguru.core.listeners.KafkaConnectionListener;
import com.loco.kafkaguru.model.KafkaClusterInfo;
import com.loco.kafkaguru.view.KafkaView;
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
public class MainWindowController implements Initializable, ControllerListener, KafkaConnectionListener {
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
    // key = cluster id
    private Map<String, KafkaClusterInfo> clusters = new HashMap<>();
    // key = controller id
    private Map<String, KafkaViewController> controllers = new HashMap<>();

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        PluginLoader.loadPlugins();
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
            var clusterIds = clustersNode.childrenNames();
            var clusters = Arrays.stream(clusterIds) //
                    .map(clusterId -> clustersNode.node(clusterId)) //
                    .map(this::createCluster)
                    .collect(Collectors.toMap((cluster -> cluster.getId()), (cluster -> cluster)));

            return clusters;
        } catch (BackingStoreException e) {
            e.printStackTrace();
        }
        return null;
    }

    private KafkaClusterInfo createCluster(Preferences preferences) {
        var clusterId = preferences.name();
        var name = preferences.get("name", null);
        var url = preferences.get("url", null);

        return new KafkaClusterInfo(clusterId, name, url);
    }

    private Map<String, KafkaViewController> createControllers(Preferences preferences,
            Map<String, KafkaClusterInfo> clusters) {
        var controllers = new TreeMap<String, KafkaViewController>();
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
            var clusterId = getClusterId(tabNode);
            var cluster = clusters.get(clusterId);
            if (cluster == null) {
                log.error(
                        "Tab with name {} could not be loaded, because "
                                + "the associated cluster could not be found in saved preferences",
                        tabNode.name(), clusterId);
                continue;
            }
            var controllerId = tabNode.name();
            if (controllerId == null) {
                log.error("Too many tabs for cluster {}", clusterId);
                continue;
            }
            var kafkaInstance = createKafkaInstance(cluster);
            var controller = new KafkaViewController(kafkaInstance, this, tabNode);
            controllers.put(controllerId, controller);
        }

        return controllers;
    }

    private String createControllerName(KafkaClusterInfo cluster) {
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

    private String getClusterId(Preferences preferences) {
        return preferences.get("cluster_id", null);
    }

    private Tab createTab(KafkaViewController controller) {
        KafkaView kafkaView = new KafkaView(controller);

        Tab tab = new Tab(controller.getName());
        tab.setId(controller.getName());
        tab.setUserData(controller);
        tab.setContent(kafkaView);

        return tab;
    }

    private void createMenuItems(Map<String, KafkaClusterInfo> clusters) {
        clusters.values().forEach(cluster -> {
            createOpenClusterMenuItem(cluster);
            createRemoveClusterMenuItem(cluster);
        });
    }

    private void createOpenClusterMenuItem(KafkaClusterInfo cluster) {
        var openClusterItem = new MenuItem(cluster.getName());
        openClusterItem.setId(cluster.getId());
        openClusterItem.setOnAction(this::onOpenCluster);
        openClusterMenu.getItems().add(openClusterItem);
    }

    private void createRemoveClusterMenuItem(KafkaClusterInfo cluster) {
        var removeClusterItem = new MenuItem(cluster.getName());
        removeClusterItem.setId(cluster.getId());
        removeClusterItem.setOnAction(this::onRemoveCluster);
        removeClusterMenu.getItems().add(removeClusterItem);
    }

    public void onNewCluster(ActionEvent actionEvent) {
        KafkaClusterInfo cluster = new KafkaClusterInfo(suggestClusterName(), "");

        clusters.put(cluster.getName(), cluster);

        var controller = createController(cluster);
        controllers.put(controller.getName(), controller);

        Tab newTab = createTab(controller);
        tabPane.getTabs().add(newTab);
        tabPane.getSelectionModel().select(newTab);
    }

    private String suggestClusterName() {
        var name = "new cluster";
        for (int i = 1; true; ++i){
            if (!clusters.containsKey(name)){
                return name;
            }
            name = "new cluster " + i;
        }
    }

    public void onNewCluster_(ActionEvent actionEvent) {
        KafkaClusterInfo cluster = getNewClusterInfo();
        if (cluster == null) {
            return;
        }
        clusters.put(cluster.getName(), cluster);
        save(cluster);

        createOpenClusterMenuItem(cluster);
        createRemoveClusterMenuItem(cluster);

        var controller = createController(cluster);
        controllers.put(controller.getName(), controller);

        Tab newTab = createTab(controller);
        tabPane.getTabs().add(newTab);
        tabPane.getSelectionModel().select(newTab);
    }

    private Preferences save(KafkaClusterInfo cluster) {
        var childPreferences = preferences.node("clusters").node(cluster.getId());
        childPreferences.put("name", cluster.getName());
        childPreferences.put("url", cluster.getUrl());
        savePreferences(childPreferences);
        return childPreferences;
    }

    public void onOpenCluster(ActionEvent event) {
        var menuItem = (MenuItem) event.getSource();

        var clusterId = menuItem.getId();
        var cluster = clusters.get(clusterId);

        var controller = createController(cluster);
        controllers.put(controller.getName(), controller);

        var tab = createTab(controller);
        tabPane.getTabs().add(tab);
    }

    private KafkaViewController createController(KafkaClusterInfo cluster) {
        var controllerName = createControllerName(cluster);
        var tabPreferences = preferences.node("tabs").node(controllerName);
        tabPreferences.put("cluster_id", cluster.getId());
        var kafkaInstance = createKafkaInstance(cluster);
        var controller = new KafkaViewController(kafkaInstance, this, tabPreferences);
        return controller;
    }

    private KafkaInstance createKafkaInstance(KafkaClusterInfo cluster) {
        var kafkaInstance = new KafkaInstance(cluster);
        kafkaInstance.addConnectionListener(this);
        return kafkaInstance;
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
    public void destroy(KafkaViewController controller) {
        tabPane.getTabs().remove(controller.getName());
        controllers.remove(controller);
    }

    @Override
    public void savePreference(ArrayList<String> nodeNames, String key, String value) {
        Preferences leafNode = preferences;

        for (var nodeName : nodeNames) {
            leafNode = leafNode.node(nodeName);
        }

        leafNode.put(key, value);

        controllers.values().forEach(c -> c.preferenceUpdated(nodeNames, key, value));
    }

    @Override
    public String getPreference(ArrayList<String> nodeNames, String key) {
        Preferences leafNode = preferences;

        for (var nodeName : nodeNames) {
            leafNode = leafNode.node(nodeName);
        }

        return leafNode.get(key, "");
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

    @Override
    public void connected(String name, boolean really) {
        var cluster = clusters.get(name);
        if (cluster == null){
            return;
        }
        save(cluster);
    }

    @Override
    public void notifyUrlChange(String name, String oldUrl, String newUrl) {
        var cluster = clusters.get(name);
        if (cluster == null){
            cluster = new KafkaClusterInfo(name, newUrl);
            clusters.put(name, cluster);
        } else {
            cluster.setUrl(newUrl);
        }
    }

    @Override
    public void notifyNameChange(String id, String oldName, String newName) {
        var cluster = clusters.get(oldName);
        if (cluster == null){
            cluster = new KafkaClusterInfo(oldName, "");
        } else {
            cluster.setName(newName);
            clusters.remove(oldName);
        }
        clusters.put(newName, cluster);

        this.controllers.keySet().stream()
                .forEach(controllerName -> {
                    var clusterName = getClusterName(controllerName);
                    if (controllerName.equals(clusterName)){
                        var controller = this.controllers.remove(controllerName);
                        var newControllerName = newName;
                        this.controllers.put(newControllerName, controller);
                        var controllerTab = tabPane.getTabs().stream().filter(tab -> tab.getUserData().equals(controller))
                        .findFirst().orElse(null);
                        if (controllerTab != null){
                            controllerTab.setText(newControllerName);
                        }
                    }
                });
    }

    private String getClusterName(String controllerName) {
        var separatorIndex = controllerName.indexOf('(');
        if (separatorIndex < 0){
            return controllerName;
        }

        return controllerName.substring(0, separatorIndex);
    }
}
