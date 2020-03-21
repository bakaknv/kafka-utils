package org.nkalugin.kafka.consumer;


import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;


/**
 * @author nkalugin on 26.11.18.
 */
public class InternalProcessInfo
{
    private final Map<TopicPartition, Long> lastProcessedRecordsTimestamps;

    private final Map<TopicPartitionOffset, Long> unprocessedRecordsTimestamps;


    public InternalProcessInfo(Map<TopicPartition, Long> lastProcessedRecordsTimestamps)
    {
        this.lastProcessedRecordsTimestamps = lastProcessedRecordsTimestamps;
        this.unprocessedRecordsTimestamps = Collections.emptyMap();
    }


    public InternalProcessInfo(Map<TopicPartition, Long> lastProcessedRecordsTimestamps,
            Map<TopicPartitionOffset, Long> unprocessedRecordsTimestamps)
    {
        this.lastProcessedRecordsTimestamps = lastProcessedRecordsTimestamps;
        this.unprocessedRecordsTimestamps = unprocessedRecordsTimestamps;
    }


    public Map<TopicPartition, Long> getLastProcessedRecordsTimestamps()
    {
        return lastProcessedRecordsTimestamps;
    }


    public Map<TopicPartitionOffset, Long> getUnprocessedRecordsTimestamps()
    {
        return unprocessedRecordsTimestamps;
    }
}
