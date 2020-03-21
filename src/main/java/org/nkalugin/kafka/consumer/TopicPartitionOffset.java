package org.nkalugin.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;


/**
 * @author nkalugin on 26.11.18.
 */
public class TopicPartitionOffset
{
    private final TopicPartition topicPartition;

    private final long offset;


    public TopicPartitionOffset(TopicPartition topicPartition, long offset)
    {
        this.topicPartition = topicPartition;
        this.offset = offset;
    }


    public TopicPartitionOffset(ConsumerRecord<?, ?> record)
    {
        this.topicPartition = new TopicPartition(record.topic(), record.partition());
        this.offset = record.offset();
    }


    public TopicPartition getTopicPartition()
    {
        return topicPartition;
    }


    public long getOffset()
    {
        return offset;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TopicPartitionOffset that = (TopicPartitionOffset) o;

        if (offset != that.offset)
            return false;
        return topicPartition != null ? topicPartition.equals(that.topicPartition) : that.topicPartition == null;
    }


    @Override
    public int hashCode()
    {
        int result = topicPartition != null ? topicPartition.hashCode() : 0;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }


    @Override
    public String toString()
    {
        return "TopicPartitionOffset{" + topicPartition + ", offset=" + offset + '}';
    }
}
