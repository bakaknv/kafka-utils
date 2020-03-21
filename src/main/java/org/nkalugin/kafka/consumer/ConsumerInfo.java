package org.nkalugin.kafka.consumer;


import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;


/**
 * Базовая информация о консюмере.
 * 
 * @author nkalugin on 16.11.18.
 */
public class ConsumerInfo<K, V>
{
    private final String clientName;

    private final Collection<TopicPartition> assignment;

    private final Deserializer<K> keyDeserializer;

    private final Deserializer<V> valueDeserializer;

    private final Map<String, Object> properties;


    public ConsumerInfo(String clientName, Collection<TopicPartition> assignment,
            Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,
            Map<String, Object> properties)
    {
        this.clientName = clientName;
        this.assignment = assignment;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.properties = properties;
    }


    public String getClientName()
    {
        return clientName;
    }


    public Collection<TopicPartition> getAssignment()
    {
        return assignment;
    }


    public Deserializer<K> getKeyDeserializer()
    {
        return keyDeserializer;
    }


    public Deserializer<V> getValueDeserializer()
    {
        return valueDeserializer;
    }


    public Map<String, Object> getAdditionalConsumerProperties()
    {
        return properties;
    }


    @Override
    public String toString()
    {
        return "ConsumerInfo{" + "clientName='" + clientName + '\'' + ", assignment=" + assignment + '}';
    }
}
