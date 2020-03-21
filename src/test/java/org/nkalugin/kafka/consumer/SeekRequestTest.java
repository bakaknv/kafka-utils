package org.nkalugin.kafka.consumer;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
public class SeekRequestTest
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
    public void testSeek() throws InterruptedException
    {

        String topic = "testSeek";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        ConsumerInfo<Void, Void> info = new ConsumerInfo<>(topic,
                Collections.singleton(topicPartition),
                new VoidDeserializer(), new VoidDeserializer(), Collections.emptyMap());

        KafkaConsumerBuilder<Void, Void> builder = kafkaBuilder(clientFactory, new VoidVoidSyncLogProcessor(info, 2))
                .withLogLagMonitoringTimeoutMs(() -> 0L)
                .withCommitIntervalMs(() -> 0L);

        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 0
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 1
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 2
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 3
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 4
        producer.flush();

        KafkaConsumingProcess<Void, Void> build = builder.build();
        // чтение начнется с офсета 2
        build.seekAndCommitRequest(topic, 0, 2);
        build.process();
        // если уехать за границы лога, то коммитнется в конец
        build.seekAndCommitRequest(topic, 0, 14);
        build.process();
        build.stop();

        Consumer<Void, Void> consumer = clientFactory.createConsumer(info);
        Assert.assertNotNull(consumer.committed(topicPartition));
        Assert.assertEquals(5L, consumer.committed(topicPartition).offset());
        consumer.close();
    }


    @Test
    public void testStatefulSeek() throws InterruptedException
    {
        String topic = "testStatefulSeek";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        ConsumerInfo<Void, Void> info = new ConsumerInfo<>(topic,
                Collections.singleton(topicPartition),
                new VoidDeserializer(), new VoidDeserializer(), Collections.emptyMap());

        ProblemVoidVoidAsyncLogProcessor asyncLogProcessor = new ProblemVoidVoidAsyncLogProcessor(info);
        KafkaConsumerBuilder<Void, Void> builder = KafkaConsumerBuilder.kafkaBuilder(clientFactory, asyncLogProcessor)
                .withLogLagMonitoringTimeoutMs(() -> 0L)
                .withCommitIntervalMs(() -> 0L);

        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 0
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 1
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 2
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 3
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 4
        producer.flush();

        KafkaConsumingProcess<Void, Void> build = builder.build();

        for (int i = 0; i < 5; i++)
        {
            try
            {
                build.process();
            }
            catch (Exception ignored)
            {
            }
        }

        // лаги у нас есть
        build.getLogLags().values().forEach(lag -> Assert.assertEquals(5L, lag.longValue()));

        // пропускаем нафиг зависшие записи
        build.seekAndCommitRequest(topic, 0, 3);
        build.process();
        build.process();
        build.stop();

        Assert.assertEquals(1, asyncLogProcessor.offsetsRetryCounter.get(0L).intValue());
        Assert.assertEquals(1, asyncLogProcessor.offsetsRetryCounter.get(1L).intValue());
        Assert.assertEquals(5, asyncLogProcessor.offsetsRetryCounter.get(2L).intValue()); // 5 повторов было только на
                                                                                          // проблемной записи
        Assert.assertEquals(1, asyncLogProcessor.offsetsRetryCounter.get(3L).intValue());
        Assert.assertEquals(1, asyncLogProcessor.offsetsRetryCounter.get(4L).intValue());

        Consumer<Void, Void> consumer = clientFactory.createConsumer(info);
        Assert.assertNotNull(consumer.committed(topicPartition));
        Assert.assertEquals(5L, consumer.committed(topicPartition).offset());
        consumer.close();
    }


    @Test
    public void testSeekToTheEnd() throws InterruptedException
    {
        String topic = "testSeekToTheEnd";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        ConsumerInfo<Void, Void> info = new ConsumerInfo<>(topic,
                Collections.singleton(topicPartition),
                new VoidDeserializer(), new VoidDeserializer(), Collections.emptyMap());

        KafkaConsumerBuilder<Void, Void> builder = kafkaBuilder(clientFactory, new VoidVoidSyncLogProcessor(info, 5))
                .withLogLagMonitoringTimeoutMs(() -> 0L)
                .withCommitIntervalMs(() -> 0L);

        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 0
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 1
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 2
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 3
        producer.send(new ProducerRecord<>(topic, 0, null, null)); // 4
        producer.flush();

        KafkaConsumingProcess<Void, Void> build = builder.build();
        build.seekToEnd(topic, 0, true);
        build.process();

        build.seekToBeginning(topic, 0, true);
        try
        {
            build.process();
        }
        catch (AssertionError ignored)
        {
            // у консюмера запрет чтения записей с офсетом меньше 4 а process сделать надо
        }

        build.stop();
        Consumer<Void, Void> consumer = clientFactory.createConsumer(info);
        Assert.assertNotNull(consumer.committed(topicPartition));
        Assert.assertEquals(1L, consumer.committed(topicPartition).offset());
        consumer.close();
    }


    private class ProblemVoidVoidAsyncLogProcessor implements AsyncLogProcessor<Void, Void>,
            CustomInitialization<Void, Void>
    {
        private final ConsumerInfo<Void, Void> info;

        private final Map<Long, Integer> offsetsRetryCounter = new HashMap<>();


        private ProblemVoidVoidAsyncLogProcessor(ConsumerInfo<Void, Void> info)
        {
            this.info = info;
        }


        @Override
        public void initialize(Consumer<Void, Void> consumer)
        {
            consumer.seekToBeginning(consumer.assignment());
        }


        @Override
        public void processRecordAsync(ConsumerRecord<Void, Void> record, Ack<Void, Void> ack)
        {
            Integer count = offsetsRetryCounter.computeIfAbsent(record.offset(), _any -> 0);
            offsetsRetryCounter.put(record.offset(), count + 1);

            if (record.offset() == 2)
            {
                throw new RuntimeException("fail");
            }

            ack.ack(record);
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

        private final long startingOffset;


        private VoidVoidSyncLogProcessor(ConsumerInfo<Void, Void> info, long startingOffset)
        {
            this.info = info;
            this.startingOffset = startingOffset;
        }


        @Override
        public void initialize(Consumer<Void, Void> consumer)
        {
            consumer.seekToBeginning(consumer.assignment());
        }


        @Override
        public void processRecordSync(ConsumerRecord<Void, Void> record)
        {
            Assert.assertTrue(record.offset() >= startingOffset);
        }


        @Override
        public ConsumerInfo<Void, Void> getConsumerInfo()
        {
            return info;
        }
    }
}
