package org.nkalugin.kafka.consumer;


import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;


/**
 * @author nkalugin on 3/21/20.
 */
public class TestClientFactory implements ConsumerFactory
{
    private final String bootstrapServers;


    public TestClientFactory(String bootstrapServers)
    {
        this.bootstrapServers = bootstrapServers;
    }


    @Override
    public <K, V> Consumer<K, V> createConsumer(String clientName, Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer, @Nullable Map<String, Object> properties)
    {
        Map<String, Object> consumerConfig = getConsumerConfig(bootstrapServers, clientName);
        if (properties != null)
        {
            consumerConfig.putAll(properties);
        }
        return new KafkaConsumer<K, V>(consumerConfig, keyDeserializer, valueDeserializer);
    }


    public static Map<String, Object> getConsumerConfig(String bootstrapServers, String clientId)
    {
        Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("enable.auto.commit", false);
        properties.put("group.id", clientId);
        properties.put("client.id", clientId);
        return properties;
    }



    public <K, V> Producer<K, V> createProducer(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer)
    {
        Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("acks", "all");
        properties.put("client.id", name);
        return new KafkaProducer<>(properties, keySerializer, valueSerializer);
    }
}
