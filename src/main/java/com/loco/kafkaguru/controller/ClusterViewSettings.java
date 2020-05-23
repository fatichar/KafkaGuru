package com.loco.kafkaguru.controller;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterViewSettings {
    private String selectedTopic;

    public static ClusterViewSettings createNew() {
        return ClusterViewSettings.builder().selectedTopic("").build();
    }
}
