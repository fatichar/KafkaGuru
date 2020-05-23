package com.loco.kafkaguru.controller;

import com.loco.kafkaguru.model.KafkaClusterInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import scala.collection.mutable.MultiMap;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MainWindowSettings {
    // key = cluster id
    private Map<String, KafkaClusterInfo> clusters;
    // key = cluster id, value = List of TabSettings objects for all
    // tabs of the cluster
    private Map<String, List<TabSettings>> clusterTabs;
    // key = cluster id, value = formatter id
    private Map<String, String> topicFormats;

    public static MainWindowSettings createNew() {
        return builder()
                .clusters(new TreeMap<>())
                .clusterTabs(new TreeMap<>())
                .topicFormats(new TreeMap<>())
                .build();
    }
}
