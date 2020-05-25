package com.loco.kafkaguru.controller;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CusterItemViewSettings {
    @Builder.Default private int batchSize = 10;
    @Builder.Default private int fetchFrom = -1;

    public static CusterItemViewSettings createNew() {
        return CusterItemViewSettings.builder().build();
    }
}
