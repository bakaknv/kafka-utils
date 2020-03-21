package org.nkalugin.kafka.consumer.async;


import org.apache.kafka.clients.consumer.ConsumerRecord;


/**
 * @author nkalugin on 17.11.18.
 */
public interface Ack<K, V>
{
    void ack(ConsumerRecord<K, V> record);
}
