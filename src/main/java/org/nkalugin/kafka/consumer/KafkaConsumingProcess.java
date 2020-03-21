package org.nkalugin.kafka.consumer;


import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.nkalugin.kafka.consumer.async.AsyncBatchLogProcessor;
import org.nkalugin.kafka.consumer.async.AsyncLogProcessor;
import org.nkalugin.kafka.consumer.sync.SyncBatchLogProcessor;
import org.nkalugin.kafka.consumer.sync.SyncLogProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Основной процесс потребления записей из кафки.
 *
 * Позволяет воспользоваться одной из реализованных стратегий обработки кафки
 * 
 * {@link SyncBatchLogProcessor}
 * {@link SyncLogProcessor}
 * {@link AsyncBatchLogProcessor}
 * {@link AsyncLogProcessor}
 *
 * или реализовать свою стратегию {@link RawLogProcessor}
 *
 * Обеспечивает удобный мониторинг, имеет возможность отключить консюмера из внешнего конфига, умеет пропускать
 * проблемные записи в логе, а также перемещать консюмера на нужную позицию.
 *
 * @author nkalugin on 16.11.18.
 */
public class KafkaConsumingProcess<K, V>
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Consumer<K, V> consumer;

    private final RawLogProcessor<K, V> rawLogProcessor;

    private final ConsumerInfo<K, V> consumerInfo;

    private final BooleanSupplier enabledSupplier;

    @Nullable
    private final LagMonitor logLagMonitor;

    private final LagMonitor timeLagMonitor;

    private volatile boolean isInitialized = false;

    private boolean needReset = false;

    @Nullable
    private volatile Exception lastException = null;

    private volatile boolean started = false;

    private volatile long lastSuccessfulExecutionTimestamp;

    private volatile boolean needResetAllData = false;

    private volatile SeekRequest seekRequest = null;

    private final Set<TopicPartitionOffset> offsetsToSkip = ConcurrentHashMap.newKeySet();

    private volatile Map<TopicPartitionOffset, Long> unprocessedRecordsTimestamps = Collections.emptyMap();


    KafkaConsumingProcess(ConsumerFactory clientFactory,
            RawLogProcessor<K, V> rawLogProcessor,
            BooleanSupplier enabledSupplier,
            @Nullable Supplier<? extends Number> lagMonitoringTimeoutMs)
    {
        this.consumerInfo = rawLogProcessor.getConsumerInfo();
        this.consumer = clientFactory.createConsumer(rawLogProcessor.getConsumerInfo());
        this.rawLogProcessor = rawLogProcessor;
        this.logLagMonitor = getLogLagMonitor(lagMonitoringTimeoutMs);
        this.timeLagMonitor = getLogLagMonitor(() -> 0L);
        this.enabledSupplier = enabledSupplier;
    }


    public synchronized void process() throws InterruptedException
    {
        try
        {
            if (!enabledSupplier.getAsBoolean())
            {
                Thread.sleep(1000);
                return;
            }

            processFirstIteration();
            processResetAllData();
            processInitialization();
            processRetryIfNeed();
            processSeekRequest();

            InternalProcessInfo internalProcessInfo = rawLogProcessor.process(consumer, offsetsToSkip);
            updateInternalProcessInfo(internalProcessInfo);

            // лаги в логах измеряем после лагов времени, чтобы не было ситуации, когда сообщений не было 10 часов,
            // а затем они появились и был момент, когда лаг в логе уже не 0 и при этом время последней обработанной
            // записи отстает на 10 часов (ситуация подозрительная на WARN)
            fetchLogLags();
            lastException = null;
            lastSuccessfulExecutionTimestamp = System.currentTimeMillis();
        }
        catch (InterruptedException e)
        {
            logger.warn("Log processor {} was interrupted while sleeping", consumerInfo.getClientName());
            throw e;
        }
        catch (Exception e)
        {
            logger.warn("Error in log processor {}", consumerInfo.getClientName(), e);
            // если ошибка возникла не при инициализации компонента -- возможно потребуется сделать reset
            if (isInitialized)
            {
                needReset = true;
            }
            lastException = e;
            throw e;
        }
    }


    private void updateInternalProcessInfo(InternalProcessInfo internalProcessInfo)
    {
        Map<TopicPartition, Long> processedRecordsTimestamps = internalProcessInfo.getLastProcessedRecordsTimestamps();
        if (processedRecordsTimestamps != null && !processedRecordsTimestamps.isEmpty())
        {
            timeLagMonitor.updateLag(() -> processedRecordsTimestamps);
        }
        unprocessedRecordsTimestamps = internalProcessInfo.getUnprocessedRecordsTimestamps();
    }


    private void processSeekRequest()
    {
        if (seekRequest != null)
        {
            logger.warn("Processing {} for {}", seekRequest, consumerInfo.getClientName());
            seekRequest.process(consumer, consumerInfo);
            rawLogProcessor.clearInternalStateOnSeekRequest(seekRequest);
            seekRequest = null;
        }
    }


    private void processResetAllData()
    {
        if (needResetAllData)
        {
            logger.warn("Resetting all data for consumer {}", consumerInfo.getClientName());
            rawLogProcessor.resetAllData(consumer);
            needResetAllData = false;
        }
    }


    private void processRetryIfNeed()
    {
        if (needReset)
        {
            rawLogProcessor.resetConsumerOnError(consumer);
            logger.info("Reset processed for {}", consumerInfo.getClientName());
            needReset = false;
        }
    }


    private void processInitialization()
    {
        if (!isInitialized)
        {
            rawLogProcessor.initialize(consumer);
            logger.info("Initialization processed for {}", consumerInfo.getClientName());
            isInitialized = true;
        }
    }


    private void processFirstIteration()
    {
        if (!started)
        {
            logger.info("Starting consuming process for {}", rawLogProcessor.getClass().getSimpleName());
            Set<TopicPartition> assignment = consumer.assignment();
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
            for (TopicPartition topicPartition : assignment)
            {
                long position = consumer.position(topicPartition);
                long lastOffset = endOffsets.getOrDefault(topicPartition, position);
                logger.warn("Topic {} @ {}, last record @ {} ({} ahead)", topicPartition, position, lastOffset,
                        lastOffset - position);
            }
            // запоминаем время перед инициализацией как "время первого запуска"
            lastSuccessfulExecutionTimestamp = System.currentTimeMillis();
            started = true;
        }
    }


    @Nullable
    private LagMonitor getLogLagMonitor(@Nullable Supplier<? extends Number> monitoringTimeoutMs)
    {
        if (monitoringTimeoutMs == null)
        {
            return null;
        }

        return new LagMonitor(monitoringTimeoutMs);
    }


    private void fetchLogLags()
    {
        if (logLagMonitor != null)
        {
            logLagMonitor.updateLag(this::fetchLogLag);
        }
    }


    private Map<TopicPartition, Long> fetchLogLag()
    {
        Set<TopicPartition> assignment = consumer.assignment();
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
        Map<TopicPartition, Long> logLags = new HashMap<>();
        endOffsets.forEach((topicPartition, endOffset) -> {
            long committed = Optional.ofNullable(consumer.committed(topicPartition)).map(OffsetAndMetadata::offset)
                    .orElse(0L);
            logLags.put(topicPartition, endOffset - committed);
        });

        logger.info("Log lags was fetched for {} : {}", consumerInfo.getClientName(), logLags);
        return logLags;
    }


    public String getClientName()
    {
        return consumerInfo.getClientName();
    }


    private Map<TopicPartition, Long> getLags(LagMonitor lagMonitor)
    {
        if (lagMonitor == null)
        {
            return Collections.emptyMap();
        }
        return lagMonitor.getOffsets();
    }


    public Map<TopicPartition, Long> getLogLags()
    {
        return getLags(this.logLagMonitor);
    }


    public Map<TopicPartition, Long> getTimeLags()
    {
        long now = System.currentTimeMillis();
        return getLags(timeLagMonitor).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> now - entry.getValue()));
    }


    @Nullable
    public Exception getLastException()
    {
        return lastException;
    }


    public long getLastSuccessfulExecutionTimestamp()
    {
        // если нас еще ни разу не запускали, говорим, что исполнялись успешно всегда-всегда
        if (!started)
        {
            return System.currentTimeMillis();
        }
        return lastSuccessfulExecutionTimestamp;
    }


    public void resetAllData()
    {
        this.needResetAllData = true;
    }


    public ConsumerInfo<K, V> getConsumerInfo()
    {
        return consumerInfo;
    }


    public void seekRequest(String topic, int partition, long offset)
    {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        assertAssigned(topicPartition);
        this.seekRequest = SeekRequest.onlySeek(topicPartition, offset);
    }


    public void seekAndCommitRequest(String topic, int partition, long offset)
    {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        assertAssigned(topicPartition);
        this.seekRequest = SeekRequest.seekAndCommit(topicPartition, offset);
    }


    public void seekToBeginning(String topic, int partition, boolean needCommit)
    {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        assertAssigned(topicPartition);
        this.seekRequest = SeekRequest.seekToBeginning(topicPartition, needCommit);
    }


    public void seekToEnd(String topic, int partition, boolean needCommit)
    {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        assertAssigned(topicPartition);
        this.seekRequest = SeekRequest.seekToEnd(topicPartition, needCommit);
    }


    private void assertAssigned(TopicPartition topicPartition)
    {
        if (!consumerInfo.getAssignment().contains(topicPartition))
        {
            throw new IllegalStateException("Consumer " + consumerInfo.getClientName() + " is not assigned to "
                    + topicPartition);
        }
    }


    public void addOffsetsToSkip(Collection<TopicPartitionOffset> offsets)
    {
        offsetsToSkip.addAll(offsets);
    }


    public void clearOffsetsToSkip()
    {
        offsetsToSkip.clear();
    }


    public Map<TopicPartitionOffset, Long> getUnprocessedRecordsTimestamps()
    {
        return unprocessedRecordsTimestamps;
    }


    public synchronized void stop()
    {
        consumer.close();
    }
}
