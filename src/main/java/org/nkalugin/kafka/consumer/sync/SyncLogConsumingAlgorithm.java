package org.nkalugin.kafka.consumer.sync;


import java.util.Queue;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.nkalugin.kafka.consumer.ConsumerInfoProvider;
import org.nkalugin.kafka.consumer.FetchResult;
import org.nkalugin.kafka.consumer.InternalProcessInfo;
import org.nkalugin.kafka.consumer.LogConsumingAlgorithm;
import org.nkalugin.kafka.consumer.SeekRequest;
import org.nkalugin.kafka.consumer.TopicPartitionOffset;


/**
 * @author nkalugin on 30.11.18.
 */
public class SyncLogConsumingAlgorithm<K, V> implements LogConsumingAlgorithm<K, V>
{
    private final java.util.function.Consumer<Queue<ConsumerRecord<K, V>>> recordsConsumer;

    private final ConsumerInfoProvider<K, V> consumerInfoProvider;

    public SyncLogConsumingAlgorithm(SyncBatchLogProcessor<K, V> syncBatchLogProcessor)
    {
        this.recordsConsumer = syncBatchLogProcessor::processBatchSync;
        this.consumerInfoProvider = syncBatchLogProcessor;
    }


    public SyncLogConsumingAlgorithm(SyncLogProcessor<K, V> syncLogProcessor)
    {
        this.recordsConsumer = consumerRecords -> {
            while (!consumerRecords.isEmpty())
            {
                ConsumerRecord<K, V> record = consumerRecords.element();
                syncLogProcessor.processRecordSync(record);
                consumerRecords.remove();
            }
        };
        this.consumerInfoProvider = syncLogProcessor;
    }


    @Override
    public ConsumerInfoProvider<K, V> getConsumerInfoProvider()
    {
        return consumerInfoProvider;
    }


    @Override
    public InternalProcessInfo processRecords(FetchResult<K, V> fetchResult, Set<TopicPartitionOffset> offsetsToSkip)
    {
        if (!fetchResult.isEmpty())
        {
            recordsConsumer.accept(fetchResult.getRecords(offsetsToSkip));
        }

        return new InternalProcessInfo(fetchResult.getLastRecordsTimestamps());
    }


    @Override
    public void commitOffsets(Consumer<K, V> consumer)
    {
        consumer.commitSync();
    }


    @Override
    public void clearInternalStateOnSeekRequest(SeekRequest seekRequest)
    {
        // nop
    }


    @Override
    public void resetAllData(Consumer<K, V> consumer)
    {
        // nop
    }
}
