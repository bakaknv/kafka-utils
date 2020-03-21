package org.nkalugin.kafka.consumer;


import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.utils.SystemTime;


/**
 * @author nkalugin on 22.11.18.
 */
public class InMemoryKafka
{
    private EmbeddedZookeeper zkServer;

    private KafkaServer kafkaServer;

    private Path tempKafkaDir;

    private String kafkaUrl;

    private String zkUrl;

    public void start()
    {
        try
        {
            // Setup Zookeeper
            zkServer = new EmbeddedZookeeper();
            zkUrl = "localhost:" + zkServer.port();

            tempKafkaDir = Files.createTempDirectory("kafka-");
            kafkaUrl = "localhost:" + getRandomFreePort();

            // Setup Broker
            Properties props = getKafkaProps(zkUrl, kafkaUrl, tempKafkaDir);
            KafkaConfig config = new KafkaConfig(props);
            kafkaServer = TestUtils.createServer(config, new SystemTime());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }


    public void stop()
    {
        kafkaServer.shutdown();
        zkServer.shutdown();
        cleanupDirectory();
    }


    private static Properties getKafkaProps(String zkUrl, String kafkaUrl, Path logDir)
    {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkUrl);
        props.put("broker.id", "0");
        props.put("offsets.topic.replication.factor", "1");
        props.put("log.dirs", logDir.toAbsolutePath().toString());
        props.put("listeners", "PLAINTEXT://" + kafkaUrl);
        props.put("transaction.state.log.replication.factor", (short) 1);
        props.put("min.insync.replicas", 1);
        props.put("transaction.state.log.min.isr", 1);
        props.put("log.cleaner.enable", false);
        props.put("group.initial.rebalance.delay.ms", 0);
        return props;
    }


    private void cleanupDirectory()
    {
        try
        {
            FileUtils.deleteDirectory(tempKafkaDir.toFile());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }


    private static String getRandomFreePort() throws IOException
    {
        try (ServerSocket ss = new ServerSocket(0))
        {
            return String.valueOf(ss.getLocalPort());
        }
    }


    public String getKafkaUrl()
    {
        return kafkaUrl;
    }
}
