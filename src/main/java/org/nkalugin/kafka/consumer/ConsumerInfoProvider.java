package org.nkalugin.kafka.consumer;


/**
 * @author nkalugin on 17.11.18.
 */
public interface ConsumerInfoProvider<K, V>
{
    /**
     * Базовая информация о консюмере
     */
    ConsumerInfo<K, V> getConsumerInfo();
}
