package org.nkalugin.kafka.consumer;


import org.apache.kafka.clients.consumer.Consumer;


/**
 * Полная очистка состояния консюмера. NB: только для целей тестирования.
 * 
 * @author nkalugin on 19.11.18.
 */
public interface ResetAllData<K, V>
{
    void resetAllData(Consumer<K, V> consumer);
}
