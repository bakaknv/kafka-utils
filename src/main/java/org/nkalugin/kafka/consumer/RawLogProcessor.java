package org.nkalugin.kafka.consumer;


import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;


/**
 * @author nkalugin on 16.11.18.
 */
public interface RawLogProcessor<K, V> extends ConsumerInfoProvider<K, V>, CustomRetry<K, V>,
        CustomInitialization<K, V>
{
    /**
     * Обработка лога в самом общем виде, имеется доступ к consumer-у, можно творить что угодно.
     * 
     * @param consumer -- консюмер
     * @param offsetsToSkip -- офсеты, которые нужно проигнорировать
     * @return -- Дополнительная информация для мониторинга состояния процессора
     */
    InternalProcessInfo process(Consumer<K, V> consumer, Set<TopicPartitionOffset> offsetsToSkip);


    /**
     * Очистка внутреннего состояния процессора при запросе перемотки указателя на какую-либо позицию.
     *
     * NB: вызывается исключительно после физического переезда косюмера, e.g. следующий poll гарантированно вернет
     * данные с позиции, указанной в реквесте
     * 
     * @param seekRequest -- куда мы только что переехали
     */
    void clearInternalStateOnSeekRequest(SeekRequest seekRequest);
}
