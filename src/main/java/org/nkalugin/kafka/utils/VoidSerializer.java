package org.nkalugin.kafka.log;


import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;


/**
 * @author nkalugin on 30.01.18.
 */
public class VoidSerializer implements Serializer<Void>
{
    @Override
    public void configure(Map<String, ?> map, boolean b)
    {
    }


    @Override
    public byte[] serialize(String topic, Void nullOnly)
    {
        return null;
    }


    @Override
    public void close()
    {
    }
}
