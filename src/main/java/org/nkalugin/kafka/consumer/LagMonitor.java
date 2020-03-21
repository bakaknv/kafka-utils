package org.nkalugin.kafka.consumer;


import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;


/**
 * @author nkalugin on 16.11.18.
 */
class LagMonitor
{
    private final TimeoutThresholdExecutor timeoutThresholdExecutor;

    private final Map<TopicPartition, Long> buffer = new ConcurrentHashMap<>();


    LagMonitor(Supplier<? extends Number> timeoutMs)
    {
        this.timeoutThresholdExecutor = new TimeoutThresholdExecutor(timeoutMs);
    }


    void updateLag(Supplier<Map<TopicPartition, Long>> offsetsProvider)
    {
        timeoutThresholdExecutor.doWithTimedThreshold(() -> {
            Map<TopicPartition, Long> offsets = offsetsProvider.get();
            buffer.putAll(offsets);
        });
    }


    Map<TopicPartition, Long> getOffsets()
    {
        return Collections.unmodifiableMap(buffer);
    }
}
