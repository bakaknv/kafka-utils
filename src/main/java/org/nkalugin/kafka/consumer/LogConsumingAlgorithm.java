package org.nkalugin.kafka.consumer;


import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;


/**
 * @author nkalugin on 30.11.18.
 */
public interface LogConsumingAlgorithm<K, V> extends ResetAllData<K, V>
{
    ConsumerInfoProvider<K, V> getConsumerInfoProvider();


    InternalProcessInfo processRecords(FetchResult<K, V> fetchResult, Set<TopicPartitionOffset> offsetsToSkip);


    void commitOffsets(Consumer<K, V> consumer);


    void clearInternalStateOnSeekRequest(SeekRequest seekRequest);
}
