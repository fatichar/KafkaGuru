package com.loco.kafkaguru.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loco.kafkaguru.core.KafkaInstance;
import com.loco.kafkaguru.core.PluginLoader;
import com.loco.kafkaguru.core.listeners.KafkaConnectionListener;
import com.loco.kafkaguru.model.KafkaClusterInfo;
import com.loco.kafkaguru.view.KafkaView;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.PartitionInfo;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
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

    // private Preferences preferences;
    private MainWindowSettings settings;
    // key = cluster id
    private Map<String, KafkaClusterInfo> clusters = new HashMap<>();
    // key = controller id
    private Map<String, KafkaViewController> controllers = new HashMap<>();

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        PluginLoader.loadPlugins();
        settings = readSettings();
        clusters = settings.getClusters();
        createMenuItems(clusters);
        controllers = new TreeMap<>();
        createControllersAndTabs(settings, controllers, tabPane.getTabs());
    }

    private MainWindowSettings readSettings() {
        var fileName = "settings.json";
        var mapper = new ObjectMapper();
        MainWindowSettings settings = null;
        try {
            settings = mapper.readValue(new File(fileName), MainWindowSettings.class);
        } catch (IOException e) {
        }
        if (settings == null) {
            settings = MigrationHelper.importSettingsFromPreferences(this.getClass().getName());
            saveSettings(settings);
        }
        if (settings == null) {
            settings = MainWindowSettings.createNew();
        }
        return settings;
    }

    private void saveSettings() {
        saveSettings(settings);
    }

    private void saveSettings(MainWindowSettings settings) {
        var fileName = "settings.json";
        var mapper = new ObjectMapper();
        try {
            mapper.writeValue(new File(fileName), settings);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createControllersAndTabs(MainWindowSettings settings, Map<String, KafkaViewController> controllers,
            ObservableList<Tab> tabs) {
        clusters.values().forEach(cluster -> {
            var clusterTabSettings = settings.getClusterTabs().get(cluster.getId());
            clusterTabSettings.stream().forEach(tabSettings -> {
                var kafkaInstance = createKafkaInstance(cluster);
                var controller = new KafkaViewController(kafkaInstance, tabSettings, settings.getTopicFormats());
                String tabId = UUID.randomUUID().toString();
                controllers.put(tabId, controller);
                var tabName = createTabName(cluster);
                var tab = createTab(tabId, tabName, controller);
                tabs.add(tab);
            });
        });
    }

    private String createTabName(KafkaClusterInfo cluster) {
        var clusterName = cluster.getName();
        var controllerName = clusterName;
        byte i = 2;
        var tabNames = tabPane.getTabs().stream()
                .filter(tab -> ((KafkaViewController) tab.getUserData()).getClusterId().equals(cluster.getId()))
                .map(tab -> tab.getText()).collect(Collectors.toSet());

        do {
            if (!tabNames.contains(controllerName)) {
                return controllerName;
            }
            controllerName = clusterName + "(" + i + ")";
        } while (++i > 0);

        return null;
    }

    private Tab createTab(String tabId, String tabName, KafkaViewController controller) {
        KafkaView kafkaView = new KafkaView(controller);

        Tab tab = new Tab(tabName);
        tab.setId(tabId);
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

        clusters.put(cluster.getId(), cluster);
        settings.getClusterTabs().put(cluster.getId(), new ArrayList<>());
        createOpenClusterMenuItem(cluster);
        createRemoveClusterMenuItem(cluster);

        var tabSettings = TabSettings.createNew();
        var controller = createController(cluster, tabSettings);
        var controllerId = UUID.randomUUID().toString();
        settings.getClusterTabs().get(cluster.getId()).add(tabSettings);
        controllers.put(controllerId, controller);

        var tabName = createTabName(cluster);
        Tab newTab = createTab(controllerId, tabName, controller);
        tabPane.getTabs().add(newTab);
        tabPane.getSelectionModel().select(newTab);

        saveSettings();
    }

    private KafkaViewController createController(KafkaClusterInfo cluster, TabSettings tabSettings) {
        var kafkaInstance = createKafkaInstance(cluster);
        var controller = new KafkaViewController(kafkaInstance, tabSettings, settings.getTopicFormats());
        return controller;
    }

    public void onOpenCluster(ActionEvent event) {
        var menuItem = (MenuItem) event.getSource();
        var clusterId = menuItem.getId();
        var cluster = clusters.get(clusterId);

        var tabSettings = TabSettings.createNew();
        var controller = createController(cluster, tabSettings);
        var controllerId = UUID.randomUUID().toString();
        settings.getClusterTabs().get(cluster.getId()).add(tabSettings);
        controllers.put(controllerId, controller);

        var tabName = createTabName(cluster);
        Tab newTab = createTab(controllerId, tabName, controller);
        tabPane.getTabs().add(newTab);
        tabPane.getSelectionModel().select(newTab);

        saveSettings();
    }

    private String suggestClusterName() {
        var name = "Cluster";
        for (int i = 1; true; ++i) {
            if (!clusters.containsKey(name)) {
                return name;
            }
            name = "Cluster " + i;
        }
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
        var clusterId = (String) menuItem.getId();
        clusters.remove(clusterId);
        removeClusterMenu.getItems().remove(menuItem);
        openClusterMenu.getItems().removeIf(item -> item.getId().equals(clusterId));

        removeTabs(clusterId);

        saveSettings();
    }

    private void removeTabs(String clusterId) {
        var tabIds = settings.getClusterTabs().remove(clusterId);

        var clusterControllerIds = controllers.entrySet().stream()
                .filter(entry -> entry.getValue().getClusterId().equals(clusterId)).map(entry -> entry.getKey())
                .collect(Collectors.toList());

        clusterControllerIds.forEach(id -> controllers.remove(id));

        tabPane.getTabs().removeIf(tab -> clusterControllerIds.contains(tab.getId()));
        settings.getClusterTabs().remove(clusterId);
    }

    public void onCloseTab(ActionEvent actionEvent) {
        log.info("Closing active tab");
        var activeTab = tabPane.getSelectionModel().getSelectedItem();
        if (activeTab == null) {
            log.info("There is no active tab");
            return;
        }

        var tabId = activeTab.getId();
        log.info("active tab title = " + activeTab.getText());
        var controller = controllers.remove(tabId);
        log.info("removed controller with id = " + tabId);
        var tabSettings = controller.getSettings();
        log.info("tabSettings = " + tabSettings);
        var clusterTabs = settings.getClusterTabs().get(controller.getClusterId());
        log.info("cluster tab count before removing = " + clusterTabs.size());
        clusterTabs.remove(tabSettings);
        var present = clusterTabs.contains(tabSettings);
        log.info("cluster tab count after removing = " + clusterTabs.size());
        tabPane.getTabs().remove(activeTab);

        saveSettings();
    }

    private boolean getConfirmation(String msg) {
        return new Alert(Alert.AlertType.CONFIRMATION, msg, ButtonType.YES, ButtonType.NO).showAndWait()
                .orElse(ButtonType.NO).equals(ButtonType.YES);
    }

    // @Override
    public void destroy(KafkaViewController controller) {
        // tabPane.getTabs().removeIf(tab -> tab.getId().equals(controller.getId()));
        // controllers.remove(controller);
    }

    @Override
    public void connectionFailed(String name) {

    }

    @Override
    public void topicsUpdated(Map<String, List<PartitionInfo>> topics) {

    }

    @Override
    public void notifyUrlChange(String clusterId, String oldUrl, String newUrl) {
        var cluster = clusters.get(clusterId);
        if (cluster == null) {
        } else {
            cluster.setUrl(newUrl);
            saveSettings();
        }
    }

    @Override
    public void notifyNameChange(String clusterId, String oldName, String newName) {
        var cluster = clusters.get(clusterId);
        if (cluster == null) {
            return;
        } else {
            cluster.setName(newName);
        }

        var clusterControllers = getControllers(clusterId);

        var tabs = tabPane.getTabs().stream().filter(tab -> clusterControllers.contains(tab.getUserData()))
                .collect(Collectors.toList());
        tabs.forEach(tab -> {
            var oldText = tab.getText();
            var newText = rename(oldText, oldName, newName);
            tab.setText(newText);
        });

        renameMenuItems(openClusterMenu, clusterId, newName);
        renameMenuItems(removeClusterMenu, clusterId, newName);

        saveSettings();
    }

    private List<KafkaViewController> getControllers(String clusterId) {
        return controllers.values().stream().filter(c -> c.getClusterId().equals(clusterId))
                .collect(Collectors.toList());
    }

    private void renameMenuItems(Menu menu, String clusterId, String newName) {
        var menuItem = menu.getItems().stream().filter(item -> clusterId.equals(item.getId())).findFirst().orElse(null);
        if (menuItem != null) {
            menuItem.setText(newName);
        }
    }

    private String rename(String oldControllerName, String oldClusterName, String newClusterName) {
        return oldControllerName.replace(oldClusterName, newClusterName);
    }
}
