package mysql.binlog.replicate.sink;

import mysql.binlog.replicate.util.Configuration;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Properties of {@link KafkaSink}.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2019/2/28.
 */
public class KafkaSinkProperties {
    private static final String KAFKA_PREFIX = "kafka.";
    private static final String KAFKA_PRODUCER_PREFIX = KAFKA_PREFIX + "producer.";
    private static final String TOPIC = KAFKA_PREFIX + "topic";
    private static final String BOOTSTRAP_SERVERS_CONFIG = KAFKA_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    private static final String DEFAULT_TOPIC = "binlog-replicate";

    private final String topic;
    private final Map<String, Object> producerProps;

    public KafkaSinkProperties(Configuration config) {
        this.topic = config.getString(TOPIC, DEFAULT_TOPIC);

        String bootStrapServers = config.getString(BOOTSTRAP_SERVERS_CONFIG);
        Validate.notBlank(bootStrapServers, "Bootstrap Servers must be specified");

        HashMap<String, Object> producerProps = new HashMap<>(4);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        producerProps.putAll(config.getSubConfigMap(KAFKA_PRODUCER_PREFIX));
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        this.producerProps = Collections.unmodifiableMap(producerProps);
    }

    public String getTopic() {
        return topic;
    }

    public Map<String, Object> getProducerProps() {
        return producerProps;
    }
}
