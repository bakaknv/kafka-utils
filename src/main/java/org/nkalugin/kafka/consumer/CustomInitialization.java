package org.nkalugin.kafka.consumer;


import org.apache.kafka.clients.consumer.Consumer;


/**
 * Интерфейс для консюмеров, которым нужно при запуске как-то модифицировать свою позицию в журнале, или гарантированно
 * произвести какие-либо действия перед обработкой журнала
 *
 * @author nkalugin on 16.11.18.
 */
public interface CustomInitialization<K, V>
{
    void initialize(Consumer<K, V> consumer);
}
