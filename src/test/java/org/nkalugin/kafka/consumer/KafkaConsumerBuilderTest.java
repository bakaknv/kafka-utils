package org.nkalugin.kafka.consumer;


import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
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
import org.nkalugin.kafka.consumer.sync.SyncBatchLogProcessor;
import org.nkalugin.kafka.consumer.sync.SyncLogProcessor;
import org.nkalugin.kafka.log.VoidDeserializer;
import org.nkalugin.kafka.log.VoidSerializer;

import static org.nkalugin.kafka.consumer.KafkaConsumerBuilder.kafkaBuilder;


/**
 * @author nkalugin on 17.11.18.
 */
public class KafkaConsumerBuilderTest
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
    public void testAsyncSingleProcessor() throws InterruptedException
    {
        Map<Long, Runnable> ack = new HashMap<>();

        String topic = "testAsync";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        ConsumerInfo<Void, Void> info = new ConsumerInfo<>(topic,
                Collections.singleton(topicPartition),
                new VoidDeserializer(), new VoidDeserializer(), Collections.emptyMap());

        KafkaConsumerBuilder<Void, Void> builder = KafkaConsumerBuilder
                .kafkaBuilder(clientFactory, new VoidVoidAsyncLogProcessor(ack, info))
                .withLogLagMonitoringTimeoutMs(() -> 0L)
                .withCommitIntervalMs(() -> 0L);

        KafkaConsumingProcess<Void, Void> build = builder.build();

        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 0
        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 1
        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 2
        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 3
        producer.flush();

        build.process();
        build.process();
        Thread.sleep(100);
        build.process();

        long logLag = build.getLogLags().get(topicPartition);
        Assert.assertEquals(4L, logLag);

        long timeLag = build.getTimeLags().get(topicPartition);
        Assert.assertTrue("time lag = " + timeLag, timeLag >= 100);
        long now = System.currentTimeMillis();
        build.getUnprocessedRecordsTimestamps().values()
                .forEach(timestamp -> Assert.assertTrue(now - timestamp >= 100));

        ack.values().forEach(Runnable::run);

        build.process();
        build.process();
        build.getLogLags().values().forEach(lag -> Assert.assertEquals(0L, (long) lag));

        Assert.assertTrue(build.getUnprocessedRecordsTimestamps().isEmpty());

        build.stop();

        Consumer<Void, Void> consumer = clientFactory.createConsumer(info);
        OffsetAndMetadata committed = consumer.committed(topicPartition);
        Assert.assertNotNull(committed);
        // офсет должен указывать на офсет последней обработанной записи + 1, e.g 4L
        Assert.assertEquals(4L, committed.offset());
        consumer.close();
    }


    @Test
    public void testContinueConsuming() throws InterruptedException
    {
        Map<Long, Runnable> ack = new HashMap<>();

        String topic = "testContinueConsuming";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        ConsumerInfo<Void, Void> info = new ConsumerInfo<>(topic,
                Collections.singleton(topicPartition),
                new VoidDeserializer(), new VoidDeserializer(), Collections.emptyMap());

        KafkaConsumerBuilder<Void, Void> builder = KafkaConsumerBuilder
                .kafkaBuilder(clientFactory, new VoidVoidAsyncLogProcessor(ack, info))
                .withLogLagMonitoringTimeoutMs(() -> 0L)
                .withCommitIntervalMs(() -> 0L);

        KafkaConsumingProcess<Void, Void> build = builder.build();

        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 0
        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 1
        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 2
        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 3
        producer.flush();

        build.process();
        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 4
        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 5
        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 6
        producer.send(new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, (Void) null)); // 7
        producer.flush();

        build.process();

        Assert.assertEquals(8, ack.size());
        ack.values().forEach(Runnable::run);

        build.process();

        Consumer<Void, Void> consumer = clientFactory.createConsumer(info);
        Assert.assertNotNull(consumer.committed(topicPartition));
        Assert.assertEquals(8L, consumer.committed(topicPartition).offset());
        consumer.close();
    }


    @Test
    public void testSyncBatchProcessor() throws InterruptedException
    {

        String topic = "testSyncBatch";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        ConsumerInfo<Void, Void> info = new ConsumerInfo<>(topic,
                Collections.singleton(topicPartition),
                new VoidDeserializer(), new VoidDeserializer(), Collections.emptyMap());

        KafkaConsumerBuilder<Void, Void> builder = KafkaConsumerBuilder
                .kafkaBuilder(clientFactory, new VoidVoidSyncBatchLogProcessor(info))
                .withLogLagMonitoringTimeoutMs(() -> 0L)
                .withCommitIntervalMs(() -> 0L);

        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 0
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 1
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 2
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 3
        producer.flush();

        KafkaConsumingProcess<Void, Void> build = builder.build();
        try
        {
            build.process();
        }
        catch (RuntimeException ignored)
        {
        }
        Assert.assertNotNull(build.getLastException());
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 4
        build.process();
        build.process();
        build.stop();

        Consumer<Void, Void> consumer = clientFactory.createConsumer(info);
        Assert.assertNotNull(consumer.committed(topicPartition));
        Assert.assertEquals(5L, consumer.committed(topicPartition).offset());
        consumer.close();
    }


    @Test
    public void testSyncSingleRecordLogProcessor() throws InterruptedException
    {

        String topic = "testSync";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        ConsumerInfo<Void, Void> info = new ConsumerInfo<>(topic,
                Collections.singleton(topicPartition),
                new VoidDeserializer(), new VoidDeserializer(), Collections.emptyMap());

        KafkaConsumerBuilder<Void, Void> builder = kafkaBuilder(clientFactory, new VoidVoidSyncLogProcessor(info))
                .withLogLagMonitoringTimeoutMs(() -> 0L)
                .withCommitIntervalMs(() -> 0L);

        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 0
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 1
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 2
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 3
        producer.flush();

        KafkaConsumingProcess<Void, Void> build = builder.build();
        for (int i = 0; i < 4; i++)
        {
            try
            {
                build.process();
            }
            catch (RuntimeException ignored)
            {
            }
            Assert.assertNotNull(build.getLastException());
        }

        build.process();
        Assert.assertNull(build.getLastException());

        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 4
        build.process();
        build.stop();

        Consumer<Void, Void> consumer = clientFactory.createConsumer(info);
        Assert.assertNotNull(consumer.committed(topicPartition));
        Assert.assertEquals(5L, consumer.committed(topicPartition).offset());
        consumer.close();
    }


    @Test
    public void testConsumerWithoutTopic() throws InterruptedException
    {
        String topic = "testConsumerWithoutTopic";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        ConsumerInfo<Void, Void> info = new ConsumerInfo<>(topic,
                Collections.singleton(topicPartition),
                new VoidDeserializer(), new VoidDeserializer(), Collections.emptyMap());

        KafkaConsumerBuilder<Void, Void> builder = kafkaBuilder(clientFactory, new SyncLogProcessor<Void, Void>()
        {
            @Override
            public void processRecordSync(ConsumerRecord<Void, Void> record)
            {
            }


            @Override
            public ConsumerInfo<Void, Void> getConsumerInfo()
            {
                return info;
            }
        }).withLogLagMonitoringTimeoutMs(() -> 0L).withCommitIntervalMs(() -> 0L);

        KafkaConsumingProcess<Void, Void> build = builder.build();
        build.process();
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 0
        producer.flush();
        build.process();
        build.stop();

        Consumer<Void, Void> consumer = clientFactory.createConsumer(info);
        Assert.assertNotNull(consumer.committed(topicPartition));
        Assert.assertEquals(1L, consumer.committed(topicPartition).offset());
        consumer.close();
    }


    private static class VoidVoidSyncBatchLogProcessor implements SyncBatchLogProcessor<Void, Void>,
            CustomInitialization<Void, Void>
    {
        private final ConsumerInfo<Void, Void> info;

        private int counter = 0;


        private VoidVoidSyncBatchLogProcessor(ConsumerInfo<Void, Void> info)
        {
            this.info = info;
        }


        @Override
        public void processBatchSync(Collection<ConsumerRecord<Void, Void>> consumerRecords)
        {
            try
            {
                if (counter == 0)
                {
                    throw new RuntimeException("No Exception");
                }
                else if (counter == 1)
                {
                    Assert.assertEquals(4, consumerRecords.size());
                }
                else if (counter == 2)
                {
                    Assert.assertEquals(1, consumerRecords.size());
                }
                else
                {
                    Assert.assertTrue(consumerRecords.isEmpty());
                }
            }
            finally
            {
                counter++;
            }
        }


        @Override
        public ConsumerInfo<Void, Void> getConsumerInfo()
        {
            return info;
        }


        @Override
        public void initialize(Consumer<Void, Void> consumer)
        {
            consumer.seekToBeginning(consumer.assignment());
        }
    }


    private static class VoidVoidSyncLogProcessor implements SyncLogProcessor<Void, Void>,
            CustomInitialization<Void, Void>
    {
        private final ConsumerInfo<Void, Void> info;

        private int counter;

        private Map<ConsumerRecord<Void, Void>, Integer> invCount = new HashMap<>();


        private VoidVoidSyncLogProcessor(ConsumerInfo<Void, Void> info)
        {
            this.info = info;
            counter = 0;
        }


        @Override
        public void processRecordSync(ConsumerRecord<Void, Void> record)
        {
            try
            {
                if (ImmutableSet.of(0, 2, 4, 6).contains(counter))
                {
                    throw new RuntimeException("No Exception");
                }
            }
            finally
            {
                counter++;
                invCount.putIfAbsent(record, 0);
                invCount.put(record, invCount.get(record) + 1);
            }
        }


        @Override
        public ConsumerInfo<Void, Void> getConsumerInfo()
        {
            return info;
        }


        @Override
        public void initialize(Consumer<Void, Void> consumer)
        {
            consumer.seekToBeginning(consumer.assignment());
        }
    }


    private class VoidVoidAsyncLogProcessor implements AsyncLogProcessor<Void, Void>, CustomInitialization<Void, Void>
    {

        private final Map<Long, Runnable> ack;

        private final ConsumerInfo<Void, Void> info;


        private VoidVoidAsyncLogProcessor(Map<Long, Runnable> ack, ConsumerInfo<Void, Void> info)
        {
            this.ack = ack;
            this.info = info;
        }


        @Override
        public void processRecordAsync(ConsumerRecord<Void, Void> record, Ack<Void, Void> ack)
        {
            System.out.println("" + record.topic() + "-" + record.partition() + " " + record.offset());
            this.ack.put(record.offset(), () -> ack.ack(record));
        }


        @Override
        public ConsumerInfo<Void, Void> getConsumerInfo()
        {
            return info;
        }


        @Override
        public void initialize(Consumer<Void, Void> consumer)
        {
            consumer.seekToBeginning(consumer.assignment());
        }
    }
}