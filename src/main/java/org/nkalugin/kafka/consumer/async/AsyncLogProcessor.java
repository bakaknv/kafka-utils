package org.nkalugin.kafka.consumer.async;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.nkalugin.kafka.consumer.ConsumerInfoProvider;


/**
 * Асинхронный поштучный обработчик лога. Позволяет отправить заявку в обработку какой-либо подсистеме и продолжить
 * чтение лога. Коммит лога будет происходить по мере выполнения внешней подсистемой заявок {@link Ack}.
 * 
 * @author nkalugin on 16.11.18.
 */
public interface AsyncLogProcessor<K, V> extends ConsumerInfoProvider<K, V>
{
    void processRecordAsync(ConsumerRecord<K, V> record, Ack<K, V> ack);
}
