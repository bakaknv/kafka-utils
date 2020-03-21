package org.nkalugin.kafka.consumer.sync;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.nkalugin.kafka.consumer.ConsumerInfoProvider;


/**
 * Синхронный поштучный обработчик лога.
 *
 * Удобная обертка над {@link SyncBatchLogProcessor}.
 *
 * Запись должна быть обработана в теле {@link this#processRecordSync(ConsumerRecord)}.
 *
 * Коммит лога будет происходить строго после обработки <b>всей пачки</b>, полученной из кафки (промежуточных коммитов
 * нет и не будет).
 *
 * @author nkalugin on 17.11.18.
 */
public interface SyncLogProcessor<K, V> extends ConsumerInfoProvider<K, V>
{
    void processRecordSync(ConsumerRecord<K, V> record);
}
