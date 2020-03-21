package org.nkalugin.kafka.consumer;


import java.util.Collections;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.nkalugin.kafka.consumer.async.Ack;
import org.nkalugin.kafka.consumer.async.AsyncLogProcessor;
import org.nkalugin.kafka.consumer.sync.SyncLogProcessor;
import org.nkalugin.kafka.log.VoidDeserializer;
import org.nkalugin.kafka.log.VoidSerializer;

import static org.nkalugin.kafka.consumer.KafkaConsumerBuilder.kafkaBuilder;


/**
 * @author nkalugin on 22.11.18.
 */
public class SkipOffsetTest
{
    private static InMemoryKafka environment;

    private static TestClientFactory clientFactory;

    private static Producer<Void, Void> producer;


    @BeforeClass
    public static void initializeKafka()
    {
        environment = new InMemoryKafka();
        environment.start();
        clientFactory = new TestClientFactory(environment.getKafkaUrl());
        producer = clientFactory.createProducer("tp", new VoidSerializer(), new VoidSerializer());
    }


    @AfterClass
    public static void shutdownKafka()
    {
        environment.stop();
    }


    @Test
    public void testSkipAsync() throws InterruptedException
    {

        String topic = "skipAsync";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        ConsumerInfo<Void, Void> info = new ConsumerInfo<>(topic,
                Collections.singleton(topicPartition),
                new VoidDeserializer(), new VoidDeserializer(), Collections.emptyMap());

        KafkaConsumerBuilder<Void, Void> builder = KafkaConsumerBuilder
                .kafkaBuilder(clientFactory, new VoidVoidAsyncLogProcessor(info))
                .withLogLagMonitoringTimeoutMs(() -> 0L)
                .withCommitIntervalMs(() -> 0L);

        KafkaConsumingProcess<Void, Void> build = builder.build();

        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 0
        producer.flush();

        build.process();
        Assert.assertEquals(1L, build.getLogLags().get(topicPartition).longValue());
        build.addOffsetsToSkip(Collections.singleton(new TopicPartitionOffset(topicPartition, 0)));
        build.process();
        Assert.assertEquals(0L, build.getLogLags().get(topicPartition).longValue());
        build.stop();

        Consumer<Void, Void> consumer = clientFactory.createConsumer(info);
        OffsetAndMetadata committed = consumer.committed(topicPartition);
        Assert.assertNotNull(committed);
        // офсет должен указывать на офсет последней обработанной записи + 1, e.g 1L
        Assert.assertEquals(1L, committed.offset());
        consumer.close();
    }


    @Test
    public void testSkipSync() throws InterruptedException
    {

        String topic = "skipSync";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        ConsumerInfo<Void, Void> info = new ConsumerInfo<>(topic,
                Collections.singleton(topicPartition),
                new VoidDeserializer(), new VoidDeserializer(), Collections.emptyMap());

        KafkaConsumerBuilder<Void, Void> builder = kafkaBuilder(clientFactory, new VoidVoidSyncLogProcessor(info))
                .withLogLagMonitoringTimeoutMs(() -> 0L)
                .withCommitIntervalMs(() -> 0L);

        KafkaConsumingProcess<Void, Void> build = builder.build();

        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 0
        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 1
        producer.flush();

        build.addOffsetsToSkip(Collections.singleton(new TopicPartitionOffset(topicPartition, 0)));

        build.process();
        Assert.assertEquals(0L, build.getLogLags().get(topicPartition).longValue());
        build.stop();

        Consumer<Void, Void> consumer = clientFactory.createConsumer(info);
        OffsetAndMetadata committed = consumer.committed(topicPartition);
        Assert.assertNotNull(committed);
        Assert.assertEquals(2L, committed.offset());
        consumer.close();
    }


    private class VoidVoidAsyncLogProcessor implements AsyncLogProcessor<Void, Void>, CustomInitialization<Void, Void>
    {
        private final ConsumerInfo<Void, Void> info;


        private VoidVoidAsyncLogProcessor(ConsumerInfo<Void, Void> info)
        {
            this.info = info;
        }


        @Override
        public void processRecordAsync(ConsumerRecord<Void, Void> record, Ack<Void, Void> ack)
        {
            // nop
        }


        @Override
        public void initialize(Consumer<Void, Void> consumer)
        {
            consumer.seekToBeginning(consumer.assignment());
        }


        @Override
        public ConsumerInfo<Void, Void> getConsumerInfo()
        {
            return info;
        }
    }


    private class VoidVoidSyncLogProcessor implements SyncLogProcessor<Void, Void>, CustomInitialization<Void, Void>
    {
        private final ConsumerInfo<Void, Void> info;


        private VoidVoidSyncLogProcessor(ConsumerInfo<Void, Void> info)
        {
            this.info = info;
        }


        @Override
        public void initialize(Consumer<Void, Void> consumer)
        {
            consumer.seekToBeginning(consumer.assignment());
        }


        @Override
        public void processRecordSync(ConsumerRecord<Void, Void> record)
        {
            Assert.assertEquals(1L, record.offset());
        }


        @Override
        public ConsumerInfo<Void, Void> getConsumerInfo()
        {
            return info;
        }
    }
}
