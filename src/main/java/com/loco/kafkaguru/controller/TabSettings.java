package com.loco.kafkaguru.controller;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TabSettings {
    @Builder.Default private double dividerPosition = 0.15;
    ClusterViewSettings clusterViewSettings;
    CusterItemViewSettings custerItemViewSettings;

    public static TabSettings createNew() {
        return TabSettings.builder()
                .clusterViewSettings(ClusterViewSettings.createNew())
                .custerItemViewSettings(CusterItemViewSettings.createNew())
                .build();
    }
}
