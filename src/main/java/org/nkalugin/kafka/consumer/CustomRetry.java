package org.nkalugin.kafka.consumer;


import org.apache.kafka.clients.consumer.Consumer;


/**
 * Интерфейс для консюмеров, которым нужен resetToSomeOffset в случае возникновения ошибки или еще какие-либо действия
 *
 * NB: не имеет смысла при работе с {@link StatefulLogProcessorContainer}, т.к. он обеспечивает повтор последней
 * прочитанной пачки/записи из лога.
 *
 * @author nkalugin on 16.11.18.
 */
public interface CustomRetry<K, V>
{
    void resetConsumerOnError(Consumer<K, V> consumer);
}
