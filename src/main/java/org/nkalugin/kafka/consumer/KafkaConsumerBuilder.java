package org.nkalugin.kafka.consumer;


import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.nkalugin.kafka.consumer.async.AsyncBatchLogProcessor;
import org.nkalugin.kafka.consumer.async.AsyncLogConsumingAlgorithm;
import org.nkalugin.kafka.consumer.async.AsyncLogProcessor;
import org.nkalugin.kafka.consumer.sync.SyncBatchLogProcessor;
import org.nkalugin.kafka.consumer.sync.SyncLogConsumingAlgorithm;
import org.nkalugin.kafka.consumer.sync.SyncLogProcessor;


/**
 * @author nkalugin on 17.11.18.
 */
public class KafkaConsumerBuilder<K, V>
{
    private final ConsumerFactory factory;

    @Nullable
    private AsyncBatchLogProcessor<K, V> asyncBatchLogProcessor;

    @Nullable
    private AsyncLogProcessor<K, V> asyncLogProcessor;

    @Nullable
    private SyncBatchLogProcessor<K, V> syncBatchLogProcessor;

    @Nullable
    private SyncLogProcessor<K, V> syncLogProcessor;

    @Nullable
    private RawLogProcessor<K, V> rawLogProcessor;

    private Supplier<? extends Number> pollTimeoutMs = () -> 1000L;

    private Supplier<? extends Number> commitIntervalMs = () -> 1000L;

    private BooleanSupplier enabledSupplier = () -> true;

    @Nullable
    private Supplier<? extends Number> logLagMonitoringTimeoutMs = () -> 10_000;


    public static <K, V> KafkaConsumerBuilder<K, V> kafkaBuilder(ConsumerFactory consumerFactory,
            RawLogProcessor<K, V> rawLogProcessor)
    {
        return new KafkaConsumerBuilder<>(consumerFactory, rawLogProcessor);
    }


    public static <K, V> KafkaConsumerBuilder<K, V> kafkaBuilder(ConsumerFactory consumerFactory,
            AsyncBatchLogProcessor<K, V> asyncLogProcessor)
    {
        return new KafkaConsumerBuilder<>(consumerFactory, asyncLogProcessor);
    }


    public static <K, V> KafkaConsumerBuilder<K, V> kafkaBuilder(ConsumerFactory consumerFactory,
            AsyncLogProcessor<K, V> asyncLogProcessor)
    {
        return new KafkaConsumerBuilder<>(consumerFactory, asyncLogProcessor);
    }


    public static <K, V> KafkaConsumerBuilder<K, V> kafkaBuilder(ConsumerFactory consumerFactory,
            SyncBatchLogProcessor<K, V> syncBatchLogProcessor)
    {
        return new KafkaConsumerBuilder<>(consumerFactory, syncBatchLogProcessor);
    }


    public static <K, V> KafkaConsumerBuilder<K, V> kafkaBuilder(ConsumerFactory consumerFactory,
            SyncLogProcessor<K, V> syncLogProcessor)
    {
        return new KafkaConsumerBuilder<>(consumerFactory, syncLogProcessor);
    }


    private KafkaConsumerBuilder(ConsumerFactory factory, RawLogProcessor<K, V> rawLogProcessor)
    {
        this.factory = factory;
        this.rawLogProcessor = rawLogProcessor;
    }


    private KafkaConsumerBuilder(ConsumerFactory factory, AsyncBatchLogProcessor<K, V> asyncBatchLogProcessor)
    {
        this.factory = factory;
        this.asyncBatchLogProcessor = Objects.requireNonNull(asyncBatchLogProcessor);
    }


    private KafkaConsumerBuilder(ConsumerFactory factory, AsyncLogProcessor<K, V> asyncLogProcessor)
    {
        this.factory = factory;
        this.asyncLogProcessor = Objects.requireNonNull(asyncLogProcessor);
    }


    private KafkaConsumerBuilder(ConsumerFactory factory, SyncBatchLogProcessor<K, V> syncBatchLogProcessor)
    {
        this.factory = factory;
        this.syncBatchLogProcessor = Objects.requireNonNull(syncBatchLogProcessor);
    }


    private KafkaConsumerBuilder(ConsumerFactory factory, SyncLogProcessor<K, V> syncLogProcessor)
    {
        this.factory = factory;
        this.syncLogProcessor = Objects.requireNonNull(syncLogProcessor);
    }



    public KafkaConsumerBuilder<K, V> withPollTimeoutMs(Supplier<? extends Number> pollTimeoutMs)
    {
        this.pollTimeoutMs = Objects.requireNonNull(pollTimeoutMs);
        return this;
    }


    public KafkaConsumerBuilder<K, V> withCommitIntervalMs(Supplier<? extends Number> commitTimeoutMs)
    {
        this.commitIntervalMs = Objects.requireNonNull(commitTimeoutMs);
        return this;
    }


    public KafkaConsumerBuilder<K, V> withEnabledSupplier(BooleanSupplier enabledSupplier)
    {
        this.enabledSupplier = Objects.requireNonNull(enabledSupplier);
        return this;
    }


    public KafkaConsumerBuilder<K, V> withLogLagMonitoringTimeoutMs(Supplier<? extends Number> logLagMonitoringTimeoutMs)
    {
        this.logLagMonitoringTimeoutMs = Objects.requireNonNull(logLagMonitoringTimeoutMs);
        return this;
    }


    public KafkaConsumerBuilder<K, V> skipLogLagMonitoring()
    {
        this.logLagMonitoringTimeoutMs = null;
        return this;
    }


    public KafkaConsumingProcess<K, V> build()
    {
        return new KafkaConsumingProcess<>(factory, createLogProcessor(), enabledSupplier, logLagMonitoringTimeoutMs);
    }


    private RawLogProcessor<K, V> createLogProcessor()
    {
        if (rawLogProcessor != null)
        {
            return rawLogProcessor;
        }
        if (asyncBatchLogProcessor != null)
        {
            return new StatefulLogProcessorContainer<>(pollTimeoutMs, commitIntervalMs,
                    new AsyncLogConsumingAlgorithm<>(asyncBatchLogProcessor));
        }
        if (asyncLogProcessor != null)
        {
            return new StatefulLogProcessorContainer<>(pollTimeoutMs, commitIntervalMs,
                    new AsyncLogConsumingAlgorithm<>(asyncLogProcessor));
        }
        if (syncBatchLogProcessor != null)
        {
            return new StatefulLogProcessorContainer<>(pollTimeoutMs, commitIntervalMs,
                    new SyncLogConsumingAlgorithm<>(syncBatchLogProcessor));
        }
        if (syncLogProcessor != null)
        {
            return new StatefulLogProcessorContainer<>(pollTimeoutMs, commitIntervalMs,
                    new SyncLogConsumingAlgorithm<>(syncLogProcessor));
        }
        throw new IllegalStateException("Log processor not found");
    }
}
