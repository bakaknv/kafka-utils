package org.nkalugin.kafka.consumer;


import java.util.Set;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;


/**
 * Процессор лога с сохранением состояния.
 *
 * При возникновении ошибки в обработке позволяет не обрабатывать уже обработанные записи.
 *
 * В случае пачечного обработчика повторяется последняя полученная из кафки пачка записей.
 *
 * В случае обработки по одной записи будет повторяться только последняя проблемная запись.
 *
 * Игнорирует интерфейс {@link CustomRetry} у обработчика лога:
 * {@link StatefulLogProcessorContainer#resetConsumerOnError}
 * 
 * @author nkalugin on 30.11.18.
 */
public class StatefulLogProcessorContainer<K, V> implements RawLogProcessor<K, V>
{
    private final Supplier<? extends Number> pollTimeoutMs;

    private final TimeoutThresholdExecutor commitController;

    private final LogConsumingAlgorithm<K, V> logConsumingAlgorithm;

    private FetchResult<K, V> fetchResult = null;


    public StatefulLogProcessorContainer(Supplier<? extends Number> pollTimeoutMs,
            Supplier<? extends Number> commitTimeoutMs,
            LogConsumingAlgorithm<K, V> logConsumingAlgorithm)
    {
        this.pollTimeoutMs = pollTimeoutMs;
        this.commitController = new TimeoutThresholdExecutor(commitTimeoutMs);
        this.logConsumingAlgorithm = logConsumingAlgorithm;
    }


    @Override
    public InternalProcessInfo process(Consumer<K, V> consumer, Set<TopicPartitionOffset> offsetsToSkip)
    {
        if (fetchResult == null)
        {
            fetchResult = new FetchResult<>(consumer.poll(pollTimeoutMs.get().longValue()));
        }

        InternalProcessInfo internalProcessInfo = logConsumingAlgorithm.processRecords(fetchResult, offsetsToSkip);

        fetchResult = null;
        commitController.doWithTimedThreshold(() -> logConsumingAlgorithm.commitOffsets(consumer));
        return internalProcessInfo;
    }


    @Override
    public void clearInternalStateOnSeekRequest(SeekRequest seekRequest)
    {
        this.fetchResult = null;
        this.logConsumingAlgorithm.clearInternalStateOnSeekRequest(seekRequest);
    }


    @Override
    public final ConsumerInfo<K, V> getConsumerInfo()
    {
        return logConsumingAlgorithm.getConsumerInfoProvider().getConsumerInfo();
    }


    @Override
    public final void initialize(Consumer<K, V> consumer)
    {
        if (logConsumingAlgorithm.getConsumerInfoProvider() instanceof CustomInitialization)
        {
            // noinspection unchecked
            ((CustomInitialization) logConsumingAlgorithm.getConsumerInfoProvider()).initialize(consumer);
        }
    }


    @Override
    public final void resetConsumerOnError(Consumer<K, V> consumer)
    {
        // nop, т.к. stateful retry
    }
}
