package org.nkalugin.kafka.log;


import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;


/**
 * @author nkalugin on 30.01.18.
 */
public class VoidDeserializer implements Deserializer<Void>
{
    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {

    }


    @Override
    public Void deserialize(String topic, byte[] data)
    {
        return null;
    }


    @Override
    public void close()
    {

    }
}
