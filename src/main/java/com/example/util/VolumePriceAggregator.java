package com.example.util;

import lombok.*;


@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VolumePriceAggregator {
    private double priceVolumeSum ;
    private double volumeSum ;
    // Add method to update the aggregator with new values
    public void add(double priceVolume, double volume) {
        priceVolumeSum += priceVolume;
        volumeSum += volume;
    }



}
