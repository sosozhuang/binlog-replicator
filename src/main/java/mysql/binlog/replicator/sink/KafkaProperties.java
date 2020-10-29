package mysql.binlog.replicator.sink;

import mysql.binlog.replicator.util.Configuration;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Properties of {@link KafkaSink}.
 *
 * @author zhuangshuo
 */
public class KafkaProperties {
    private static final String DEFAULT_TOPIC = "binlog-replicator";
    private final String topic;
    private final Map<String, String> tableTopicMap;
    private final Map<String, Object> producerProps;

    public KafkaProperties(Configuration config) {
        this.topic = config.getString("topic", DEFAULT_TOPIC);

        HashMap<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventSerializer.class.getName());
        producerProps.putAll(config.getSubConfigMap("producer"));
        this.producerProps = Collections.unmodifiableMap(producerProps);
    }

    String getTopic() {
        return topic;
    }

    Map<String, Object> getProducerProps() {
        return producerProps;
    }

    Map<String, String> getTableTopicMap() {
        return tableTopicMap;
    }

    @Override
    public String toString() {
        return "KafkaProperties{" +
                "topic='" + topic + '\'' +
                ", tableTopicMap=" + tableTopicMap +
                ", producerProps=" + producerProps +
                '}';
    }
}
