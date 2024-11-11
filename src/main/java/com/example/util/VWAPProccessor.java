package com.example.util;

import com.example.dto.CurrencyInfo;
import com.example.dto.VWAP;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

@Slf4j
public class VWAPProccessor implements Processor<String, CurrencyInfo> {
    private ProcessorContext context;
    private KeyValueStore<String, VolumePriceAggregator> stateStore;
    private Cancellable punctuator;
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        // Access the state store
        this.stateStore = (KeyValueStore<String, VolumePriceAggregator>) context.getStateStore("vwap-store");

        // Schedule periodic task every minute
        this.punctuator = context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            performPeriodicAggregation(timestamp);
        });
    }

    @Override
    public void process(String key, CurrencyInfo currencyInfo) {
        VolumePriceAggregator aggregator = stateStore.get(key);
        if (aggregator == null){
            aggregator =  new VolumePriceAggregator(0.0, 0.0);
        }
        aggregator.add(currencyInfo.getVolume()*currencyInfo.getPrice(), currencyInfo.getVolume());
        log.info("Adding following fields: sum of volume = {} sum of volume*price={} to store", aggregator.getVolumeSum(), aggregator.getPriceVolumeSum());

        stateStore.put(currencyInfo.getCurrencyPair(), aggregator);
    }

    public double getVWAP(double priceVolumeSum, double volumeSum) {
        return volumeSum > 0 ? priceVolumeSum / volumeSum : 0;
    }

    private void performPeriodicAggregation(long timestamp) {
        log.info("Processing periodic vwap aggregation");
        KeyValueIterator<String, VolumePriceAggregator> iter = stateStore.all();
        while (iter.hasNext()) {
            KeyValue<String, VolumePriceAggregator> entry = iter.next();
            String currencyPair = entry.key;
            double vwapValue = getVWAP( entry.value.getPriceVolumeSum(), entry.value.getVolumeSum());
            VWAP vwap = VWAP.newBuilder()
                    .setTimestamp(timestamp)
                    .setCurrencyPair(currencyPair)
                    .setVwap(vwapValue)
                    .build();
            log.info("Outputing following fields: timestamp = {} currency pair = {}, vwap = {} to output topic", vwap.getTimestamp(), vwap.getCurrencyPair(), vwap.getVwap());
            context.forward(currencyPair, vwap);
            stateStore.delete(entry.key);
        }
        iter.close();
        context.commit();
    }

    @Override
    public void close() {
        punctuator.cancel();

    }
}
