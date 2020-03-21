package org.nkalugin.kafka.consumer.async;


import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.nkalugin.kafka.consumer.ConsumerInfoProvider;


/**
 * Асинхронный пачечный обработчик лога. Позволяет отправить пачку в обработку какой-либо подсистеме и продолжить чтение
 * лога. Коммит лога будет происходить по мере выполнения внешней подсистемой заявок {@link Ack}.
 *
 * @author nkalugin on 17.11.18.
 */
public interface AsyncBatchLogProcessor<K, V> extends ConsumerInfoProvider<K, V>
{
    void processBatchAsync(Collection<ConsumerRecord<K, V>> batch, Ack<K, V> ack);
}
