package org.nkalugin.kafka.consumer;


import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;


/**
 * @author nkalugin on 17.11.18.
 */
public class FetchResult<K, V>
{
    private final Map<TopicPartition, Long> lastRecordsTimestamps;

    private final Queue<ConsumerRecord<K, V>> records;


    FetchResult(ConsumerRecords<K, V> consumerRecords)
    {
        this.records = new ArrayDeque<>(consumerRecords.count());
        this.lastRecordsTimestamps = new HashMap<>(consumerRecords.partitions().size());

        for (ConsumerRecord<K, V> record : consumerRecords)
        {
            records.add(record);
        }
        for (TopicPartition partition : consumerRecords.partitions())
        {
            List<ConsumerRecord<K, V>> partitionRecords = consumerRecords.records(partition);
            if (!partitionRecords.isEmpty())
            {
                ConsumerRecord<K, V> lastRecord = partitionRecords.get(partitionRecords.size() - 1);
                lastRecordsTimestamps.put(partition, lastRecord.timestamp());
            }
        }
    }


    public boolean isEmpty()
    {
        return records.isEmpty();
    }


    public Map<TopicPartition, Long> getLastRecordsTimestamps()
    {
        return lastRecordsTimestamps;
    }


    public Queue<ConsumerRecord<K, V>> getRecords(Set<TopicPartitionOffset> offsetsToSkip)
    {
        if (offsetsToSkip.isEmpty())
        {
            return records;
        }

        return records.stream().filter(record -> !offsetsToSkip.contains(new TopicPartitionOffset(record)))
                .collect(Collectors.toCollection(ArrayDeque::new));
    }
}
