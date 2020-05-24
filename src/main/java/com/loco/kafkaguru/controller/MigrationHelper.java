package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.model.KafkaClusterInfo;
import lombok.extern.log4j.Log4j2;

import java.util.*;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;
import java.util.stream.Collectors;

@Log4j2
public class MigrationHelper {
    public static MainWindowSettings importSettingsFromPreferences(String rootNodeName) {
        try {
            var userRoot = Preferences.userRoot();
            var oldSettingsPresent = userRoot.nodeExists(rootNodeName);
            if (!oldSettingsPresent) {
                return null;
            }
            return createMainWindowSettings(userRoot.node(rootNodeName));
        } catch (BackingStoreException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static MainWindowSettings createMainWindowSettings(Preferences preferences) {
        var clusters = readClusters(preferences);
        var tabs = readTabs(clusters, preferences);
        Map<String, String> topicFormats = readTopicFormats(preferences);

        var settings =
                MainWindowSettings.builder()
                        .clusters(clusters)
                        .clusterTabs(tabs)
                        .topicFormats(topicFormats)
                        .build();
        return settings;
    }

    private static Map<String, KafkaClusterInfo> readClusters(Preferences appPreferences) {
        try {
            var clustersNode = appPreferences.node("clusters");
            var clusterNames = clustersNode.childrenNames();
            var clusters =
                    Arrays.stream(clusterNames) //
                            .map(clusterName -> clustersNode.node(clusterName)) //
                            .map(MigrationHelper::readCluster)
                            .collect(
                                    Collectors.toMap(
                                            (cluster -> cluster.getId()), (cluster -> cluster)));

            return clusters;
        } catch (BackingStoreException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static KafkaClusterInfo readCluster(Preferences preferences) {
        var clusterName = preferences.name();
        var url = preferences.get("url", null);

        return new KafkaClusterInfo(clusterName, url);
    }

    private static Map<String, List<TabSettings>> readTabs(
            Map<String, KafkaClusterInfo> clusters, Preferences preferences) {
        Map<String, List<TabSettings>> clusterTabs = new TreeMap<>();
        List<Preferences> tabNodes = null;
        try {
            var tabsNode = preferences.node("tabs");
            var tabIds = tabsNode.childrenNames();
            tabNodes =
                    Arrays.stream(tabIds) //
                            .map(tabId -> tabsNode.node(tabId)) //
                            .collect(Collectors.toList());
        } catch (BackingStoreException e) {
            e.printStackTrace();
            return clusterTabs;
        }

        for (var tabNode : tabNodes) {
            var clusterName = tabNode.get("cluster_id", null);
            ;
            var cluster = clusters.values().stream()
                    .filter(c -> c.getName().equals(clusterName))
                    .findFirst().orElse(null);
            if (cluster == null) {
                log.error(
                        "Tab with name {} could not be loaded, because "
                                + "the cluster {} could not be found in saved preferences",
                        tabNode.name(),
                        clusterName);
                continue;
            }

            var tabSettings = TabSettings.createNew();

            var tabs = clusterTabs.getOrDefault(cluster.getId(), new ArrayList<>());
            tabs.add(tabSettings);
            clusterTabs.put(cluster.getId(), tabs);
        }

        return clusterTabs;
    }

    private static Map<String, String> readTopicFormats(Preferences preferences) {
        Map<String, String> topicFormats = new TreeMap<>();
        var topicsNode = preferences.node("topics");
        try {
            var topics = topicsNode.childrenNames();
            Arrays.stream(topics)
                    .forEach(
                            topic -> {
                                var topicNode = topicsNode.node(topic);
                                var formatter = topicNode.get("formatter", null);
                                if (formatter != null) {
                                    topicFormats.put(topic, formatter);
                                }
                            });
        } catch (BackingStoreException e) {
            e.printStackTrace();
        }

        return topicFormats;
    }
}
