package org.nkalugin.kafka.consumer.async;


import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.nkalugin.kafka.consumer.TopicPartitionOffset;


/**
 * @author nkalugin on 16.11.18.
 */
class MultiTopicsOffsetController<K, V> implements OffsetController<K, V>
{
    private final SortedSetMultimap<TopicPartition, Long> topicOffsets = TreeMultimap.create(Comparator
            .comparing(TopicPartition::topic).thenComparing(TopicPartition::partition), Long::compareTo);

    private final Map<TopicPartition, Long> maxSeenRecords = new HashMap<>();

    private final Map<TopicPartitionOffset, Long> recordsTimestamps = new HashMap<>();


    @Override
    public void recordsSeen(Collection<ConsumerRecord<K, V>> records)
    {
        records.forEach(record -> {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            topicOffsets.put(topicPartition, record.offset());
            recordsTimestamps.put(new TopicPartitionOffset(topicPartition, record.offset()), System.currentTimeMillis());
            maxSeenRecords.merge(topicPartition, record.offset(), Math::max);
        });
    }


    @Override
    public void recordsProcessed(Queue<ConsumerRecord<K, V>> ackQueue)
    {
        while (!ackQueue.isEmpty())
        {
            ConsumerRecord<K, V> record = ackQueue.peek();
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            recordProcessed(topicPartition, record.offset());
            ackQueue.poll();
        }
    }


    @Override
    public void recordsProcessed(Set<TopicPartitionOffset> offsetsToSkip)
    {
        offsetsToSkip.forEach(skipOffset -> recordProcessed(skipOffset.getTopicPartition(), skipOffset.getOffset()));
    }


    private void recordProcessed(TopicPartition topicPartition, long offset)
    {
        SortedSet<Long> offsets = topicOffsets.get(topicPartition);
        if (offsets != null)
        {
            offsets.remove(offset);
            recordsTimestamps.remove(new TopicPartitionOffset(topicPartition, offset));
        }
    }


    @Override
    public Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets()
    {
        if (maxSeenRecords.isEmpty() && topicOffsets.isEmpty())
        {
            return Collections.emptyMap();
        }

        Map<TopicPartition, Long> committed = new HashMap<>();
        for (TopicPartition topicPartition : Sets.union(topicOffsets.keySet(), maxSeenRecords.keySet()))
        {
            SortedSet<Long> offsets = topicOffsets.get(topicPartition);
            committed.put(topicPartition, computeLastProcessedRecord(topicPartition, offsets));
        }

        return committed.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> new OffsetAndMetadata(entry.getValue())));
    }


    @Override
    public Map<TopicPartitionOffset, Long> getRecordsTimestamps()
    {
        return Collections.unmodifiableMap(recordsTimestamps);
    }


    private long computeLastProcessedRecord(TopicPartition partition, SortedSet<Long> offsets)
    {
        if (offsets.isEmpty())
        {
            return maxSeenRecords.get(partition) + 1;
        }
        else
        {
            return offsets.first();
        }
    }


    @Override
    public void clear()
    {
        this.topicOffsets.clear();
        this.recordsTimestamps.clear();
        this.maxSeenRecords.clear();
    }
}
