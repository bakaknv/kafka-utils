package org.nkalugin.kafka.consumer.async;


import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.nkalugin.kafka.consumer.TopicPartitionOffset;


/**
 * @author nkalugin on 05.12.18.
 */
class SingleTopicOffsetController<K, V> implements OffsetController<K, V>
{
    private final TopicPartition topicPartition;

    private final SortedSet<Long> offsets = new TreeSet<>();

    private final Map<Long, Long> recordsTimestamps = new HashMap<>();

    private long maxSeenRecord = -1;


    SingleTopicOffsetController(TopicPartition topicPartition)
    {
        this.topicPartition = topicPartition;
    }


    @Override
    public void recordsSeen(Collection<ConsumerRecord<K, V>> records)
    {
        records.forEach(record -> {
            long offset = record.offset();
            offsets.add(offset);
            maxSeenRecord = Math.max(maxSeenRecord, offset);
        });
    }


    @Override
    public void recordsProcessed(Queue<ConsumerRecord<K, V>> ackQueue)
    {
        while (!ackQueue.isEmpty())
        {
            ConsumerRecord<K, V> record = ackQueue.peek();
            offsets.remove(record.offset());
            ackQueue.poll();
        }
    }


    @Override
    public void recordsProcessed(Set<TopicPartitionOffset> offsetsToSkip)
    {
        offsetsToSkip.forEach(topicPartitionOffset -> {
            if (topicPartitionOffset.getTopicPartition().equals(topicPartition))
            {
                offsets.remove(topicPartitionOffset.getOffset());
            }
        });
    }


    @Override
    public Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets()
    {
        return Collections.singletonMap(topicPartition, new OffsetAndMetadata(
                offsets.isEmpty() ? maxSeenRecord + 1 : offsets.first()
                ));
    }


    @Override
    public Map<TopicPartitionOffset, Long> getRecordsTimestamps()
    {
        return recordsTimestamps.entrySet().stream().collect(
                Collectors.toMap(entry -> new TopicPartitionOffset(topicPartition, entry.getKey()),
                        Map.Entry::getValue));
    }


    @Override
    public void clear()
    {
        this.recordsTimestamps.clear();
        this.offsets.clear();
        this.maxSeenRecord = -1;
    }
}
