package org.nkalugin.kafka.consumer;


import java.util.Map;

import javax.annotation.Nullable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Deserializer;


/**
 * @author nkalugin on 16.11.18.
 */
public interface ConsumerFactory
{
    default <K, V> Consumer<K, V> createConsumer(ConsumerInfo<K, V> consumerInfo)
    {
        Consumer<K, V> consumer = createConsumer(consumerInfo.getClientName(),
                consumerInfo.getKeyDeserializer(),
                consumerInfo.getValueDeserializer(),
                consumerInfo.getAdditionalConsumerProperties());
        consumer.assign(consumerInfo.getAssignment());
        return consumer;
    }


    /**
     * Создает consumer с указанными именем и десериализаторами. При создании применяются свойства по умолчанию, которые
     * переопределяются свойствами, переданными в аргументе properties.
     */
    <K, V> Consumer<K, V> createConsumer(String clientName, Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer, @Nullable Map<String, Object> properties);
}
