package org.nkalugin.kafka.consumer.async;


import java.util.Collection;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.nkalugin.kafka.consumer.ConsumerInfoProvider;
import org.nkalugin.kafka.consumer.FetchResult;
import org.nkalugin.kafka.consumer.InternalProcessInfo;
import org.nkalugin.kafka.consumer.LogConsumingAlgorithm;
import org.nkalugin.kafka.consumer.SeekRequest;
import org.nkalugin.kafka.consumer.TopicPartitionOffset;


/**
 * @author nkalugin on 30.11.18.
 */
public class AsyncLogConsumingAlgorithm<K, V> implements LogConsumingAlgorithm<K, V>
{
    private final BiConsumer<Queue<ConsumerRecord<K, V>>, Ack<K, V>> recordsConsumer;

    private final ConsumerInfoProvider<K, V> consumerInfoProvider;

    private final Queue<ConsumerRecord<K, V>> ackQueue = new ConcurrentLinkedQueue<>();

    private final OffsetController<K, V> offsetController;


    public AsyncLogConsumingAlgorithm(AsyncBatchLogProcessor<K, V> asyncBatchLogProcessor)
    {
        this.recordsConsumer = asyncBatchLogProcessor::processBatchAsync;
        this.consumerInfoProvider = asyncBatchLogProcessor;
        this.offsetController = createController(asyncBatchLogProcessor);
    }


    public AsyncLogConsumingAlgorithm(AsyncLogProcessor<K, V> asyncLogProcessor)
    {
        this.recordsConsumer = (consumerRecords, kvAck) -> {
            while (!consumerRecords.isEmpty())
            {
                ConsumerRecord<K, V> record = consumerRecords.element();
                asyncLogProcessor.processRecordAsync(record, kvAck);
                consumerRecords.remove();
            }
        };
        this.consumerInfoProvider = asyncLogProcessor;
        this.offsetController = createController(asyncLogProcessor);
    }


    private OffsetController<K, V> createController(ConsumerInfoProvider<K, V> consumerInfoProvider)
    {
        Collection<TopicPartition> assignment = consumerInfoProvider.getConsumerInfo().getAssignment();
        if (assignment.size() == 1)
        {
            return new SingleTopicOffsetController<>(assignment.iterator().next());
        }
        return new MultiTopicsOffsetController<>();
    }


    @Override
    public ConsumerInfoProvider<K, V> getConsumerInfoProvider()
    {
        return consumerInfoProvider;
    }


    @Override
    public InternalProcessInfo processRecords(FetchResult<K, V> fetchResult, Set<TopicPartitionOffset> offsetsToSkip)
    {
        offsetController.recordsSeen(fetchResult.getRecords(offsetsToSkip));

        if (!fetchResult.isEmpty())
        {
            recordsConsumer.accept(fetchResult.getRecords(offsetsToSkip), ackQueue::add);
        }

        offsetController.recordsProcessed(offsetsToSkip);
        offsetController.recordsProcessed(ackQueue);
        return new InternalProcessInfo(fetchResult.getLastRecordsTimestamps(), offsetController.getRecordsTimestamps());
    }


    @Override
    public void commitOffsets(Consumer<K, V> consumer)
    {
        consumer.commitSync(offsetController.getCommittedOffsets());
    }


    @Override
    public void clearInternalStateOnSeekRequest(SeekRequest seekRequest)
    {
        offsetController.clear();
    }
}
