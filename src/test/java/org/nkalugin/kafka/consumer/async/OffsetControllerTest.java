package org.nkalugin.kafka.consumer.async;


import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;


/**
 * @author nkalugin on 17.11.18.
 */
public class OffsetControllerTest
{
    private final Queue<ConsumerRecord<Void, Void>> ackQueue = new ArrayDeque<>();


    private ConsumerRecord<Void, Void> nextRecord(TopicPartition partition, long offset)
    {
        return new ConsumerRecord<>(partition.topic(), partition.partition(), offset, null, null);
    }


    @Test
    public void testMultiTopicOffsetController()
    {
        OffsetController<Void, Void> controller = new MultiTopicsOffsetController<>();

        Assert.assertTrue(controller.getCommittedOffsets().isEmpty());

        List<ConsumerRecord<Void, Void>> records = Arrays.asList(
                nextRecord(new TopicPartition("1", 0), 1),
                nextRecord(new TopicPartition("1", 1), 1),
                nextRecord(new TopicPartition("1", 2), 1),
                nextRecord(new TopicPartition("1", 0), 2),
                nextRecord(new TopicPartition("1", 1), 2),
                nextRecord(new TopicPartition("1", 0), 3)
                );

        controller.recordsSeen(records);

        controller.getCommittedOffsets().forEach(
                ((topicPartition, offsetAndMetadata) -> Assert.assertEquals(1L, offsetAndMetadata.offset())));

        // обработали запись с офсетом 1, можно коммитить на 2
        processRecordAndAssertOffset(controller, records.get(0), 2);
        // at-least-once
        processRecordAndAssertOffset(controller, records.get(0), 2);
        // обработали запись с офсетом 3, все еще можно коммитить на позицию 2 (ее не подтверждали)
        processRecordAndAssertOffset(controller, records.get(5), 2);
        // обработали все записи из партиции 1 (1, 2, 3) -- можно коммитить на 4ю позицию
        processRecordAndAssertOffset(controller, records.get(3), 4);

        processRecordAndAssertOffset(controller, records.get(2), 2);
        processRecordAndAssertOffset(controller, records.get(4), 1);
        processRecordAndAssertOffset(controller, records.get(1), 3);
    }


    @Test
    public void testSingleTopicOffsetController()
    {
        TopicPartition topicPartition = new TopicPartition("1", 0);
        OffsetController<Void, Void> controller = new SingleTopicOffsetController<>(topicPartition);

        List<ConsumerRecord<Void, Void>> records = Arrays.asList(
                nextRecord(new TopicPartition("1", 0), 1),
                nextRecord(new TopicPartition("1", 0), 2),
                nextRecord(new TopicPartition("1", 0), 3),
                nextRecord(new TopicPartition("1", 0), 4),
                nextRecord(new TopicPartition("1", 0), 5),
                nextRecord(new TopicPartition("1", 0), 6)
                );

        controller.recordsSeen(records);

        controller.getCommittedOffsets().forEach(
                ((tp, offsetAndMetadata) -> Assert.assertEquals(1L, offsetAndMetadata.offset())));

        // обработали запись с офсетом 1, можно коммитить на 2
        processRecordAndAssertOffset(controller, records.get(0), 2);
        // at-least-once
        processRecordAndAssertOffset(controller, records.get(0), 2);
        // обработали запись с офсетом 5, все еще можно коммитить на позицию 2 (ее не подтверждали)
        processRecordAndAssertOffset(controller, records.get(5), 2);
        processRecordAndAssertOffset(controller, records.get(2), 2);
        processRecordAndAssertOffset(controller, records.get(1), 4);
        processRecordAndAssertOffset(controller, records.get(4), 4);
        processRecordAndAssertOffset(controller, records.get(3), 7);

    }


    private void processRecordAndAssertOffset(OffsetController<Void, Void> controller,
            ConsumerRecord<Void, Void> record, long expected)
    {
        ackQueue.add(record);
        controller.recordsProcessed(ackQueue);
        OffsetAndMetadata offsetAndMetadata = controller.getCommittedOffsets()
                .get(new TopicPartition(record.topic(), record.partition()));
        Assert.assertNotNull(offsetAndMetadata);
        Assert.assertEquals(expected, offsetAndMetadata.offset());
    }
}