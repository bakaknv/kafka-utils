package org.nkalugin.kafka.consumer;


import java.util.Collections;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author nkalugin on 22.11.18.
 */
public class SeekRequest
{
    private static final long TO_BEGINNING = Long.MIN_VALUE;

    private static final long TO_END = Long.MAX_VALUE;

    private static final Logger logger = LoggerFactory.getLogger(SeekRequest.class);

    private final TopicPartition topicPartition;

    private final long offsetToSeek;

    private final boolean needCommit;


    static SeekRequest onlySeek(TopicPartition topicPartition, long offset)
    {
        return new SeekRequest(topicPartition, offset, false);
    }


    static SeekRequest seekAndCommit(TopicPartition topicPartition, long offset)
    {
        return new SeekRequest(topicPartition, offset, true);
    }


    static SeekRequest seekToBeginning(TopicPartition topicPartition, boolean needCommit)
    {
        return new SeekRequest(topicPartition, TO_BEGINNING, needCommit);
    }


    static SeekRequest seekToEnd(TopicPartition topicPartition, boolean needCommit)
    {
        return new SeekRequest(topicPartition, TO_END, needCommit);
    }


    private SeekRequest(TopicPartition topicPartition, long offsetToSeek, boolean needCommit)
    {
        if (offsetToSeek < 0 && offsetToSeek != TO_BEGINNING)
        {
            throw new IllegalStateException("Offset can't be negative");
        }
        this.topicPartition = topicPartition;
        this.offsetToSeek = offsetToSeek;
        this.needCommit = needCommit;
    }


    void process(Consumer<?, ?> consumer, ConsumerInfo<?, ?> consumerInfo)
    {
        if (!consumer.assignment().contains(topicPartition))
        {
            logger.warn("{} was skipped for {} due to assignment", this, consumerInfo.getClientName());
            return;
        }

        if (offsetToSeek == TO_BEGINNING)
        {
            consumer.seekToBeginning(Collections.singleton(topicPartition));
        }
        else if (offsetToSeek == TO_END)
        {
            consumer.seekToEnd(Collections.singleton(topicPartition));
        }
        else
        {
            consumer.seek(topicPartition, offsetToSeek);
        }

        if (needCommit)
        {
            consumer.commitSync(Collections.singletonMap(topicPartition,
                    new OffsetAndMetadata(consumer.position(topicPartition) + 1)));
        }
    }


    @Override
    public String toString()
    {
        return "SeekRequest{" + "topicPartition=" + topicPartition + ", offsetToSeek=" + offsetToSeek + ", needCommit="
                + needCommit + '}';
    }
}
