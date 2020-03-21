package org.nkalugin.kafka.consumer.async;


import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.nkalugin.kafka.consumer.TopicPartitionOffset;


/**
 * @author nkalugin on 05.12.18.
 */
public interface OffsetController<K, V>
{
    void recordsSeen(Collection<ConsumerRecord<K, V>> records);


    void recordsProcessed(Queue<ConsumerRecord<K, V>> ackQueue);


    void recordsProcessed(Set<TopicPartitionOffset> offsetsToSkip);


    Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets();


    Map<TopicPartitionOffset, Long> getRecordsTimestamps();


    void clear();
}
