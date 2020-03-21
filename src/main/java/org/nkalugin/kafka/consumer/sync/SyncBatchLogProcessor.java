package org.nkalugin.kafka.consumer.sync;


import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.nkalugin.kafka.consumer.ConsumerInfoProvider;


/**
 * Синхронный пачечный обработчик лога.
 *
 * Все записи должны быть обработаны в теле {@link this#processBatchSync(Collection)}.
 *
 * Коммит лога будет происходить строго после обработки всей пачки.
 * 
 * @author nkalugin on 17.11.18.
 */
public interface SyncBatchLogProcessor<K, V> extends ConsumerInfoProvider<K, V>
{
    void processBatchSync(Collection<ConsumerRecord<K, V>> records);
}
