package mysql.binlog.replicator.sink;

import mysql.binlog.replicator.util.Configuration;
import mysql.binlog.replicator.util.RegexHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;


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
        String tableTopics = config.getString("tableTopics");
        if (StringUtils.isBlank(tableTopics)) {
            this.tableTopicMap = Collections.emptyMap();
        } else {
            this.tableTopicMap = new RegexHashMap<>();
            initTableTopicMap(tableTopics);
        }

        HashMap<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventSerializer.class.getName());
        producerProps.putAll(config.getSubConfigMap("producer"));
        this.producerProps = Collections.unmodifiableMap(producerProps);
    }

    private void initTableTopicMap(String tableTopics) {
        Stream.of(tableTopics.split(",")).filter(StringUtils::isNotBlank).forEach(item -> {
            String[] result = item.split(":");
            Validate.isTrue(result.length == 2, "illegal tableTopics");
            Validate.notBlank(result[0], "table cannot be empty string");
            Validate.notBlank(result[1], "topic cannot be empty string");
            tableTopicMap.put(result[0].trim(), result[1].trim());
        });
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
