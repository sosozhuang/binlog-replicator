package mysql.binlog.replicator.sink;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;
import mysql.binlog.replicator.concurrent.FailedFuture;
import mysql.binlog.replicator.metric.MetricHolder;
import mysql.binlog.replicator.metric.MetricStore;
import mysql.binlog.replicator.model.ReplicatorEvent;
import mysql.binlog.replicator.model.ReplicatorMessage;
import mysql.binlog.replicator.util.AbstractLifeCycle;
import mysql.binlog.replicator.util.ByteUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author zhuangshuo
 */
public class KafkaSink extends AbstractLifeCycle implements Sink<ReplicatorMessage>, MetricHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);
    private static final String EVENT_PROCESSED_TOTAL = "event_processed_total";
    private static final String PROCESSED_TOTAL_HELP = "Total number of processed events.";
    private static final List<String> LABEL_NAMES = Arrays.asList("collector", "destination");
    private static final String PROCESS_DURATION = "process_duration";
    private static final String PROCESS_DURATION_HELP = "Process duration in milliseconds.";
    private static final List<String> PROCESS_DURATION_LABEL_NAMES = Arrays.asList("collector", "type", "destination");
    private static int counter = 0;
    private final List<String> labelValues;
    private final List<String> sendDurationLabelValues;
    private final String metricRootPath;
    private final KafkaProperties kafkaProperties;
    private final MetricStore metricStore;
    private final KafkaProducer<String, Object> kafkaProducer;
    private volatile long total;
    private volatile long sendDuration;

    public KafkaSink(KafkaProperties kafkaProperties, ClientIdentity clientId, MetricStore metricStore) {
        super(counter);
        this.kafkaProperties = kafkaProperties;
        String collector = "kafka_sink_" + counter;
        counter++;
        this.metricRootPath = MetricStore.getPath(clientId, collector);
        this.labelValues = Arrays.asList(collector, clientId.getDestination());
        this.sendDurationLabelValues = Arrays.asList(collector, "send", clientId.getDestination());
        this.metricStore = Objects.requireNonNull(metricStore);
        this.kafkaProducer = new KafkaProducer<>(kafkaProperties.getProducerProps());
        this.total = 0;
        this.sendDuration = 0;
    }

    @Override
    public void process(ReplicatorMessage message) {
        long start = System.currentTimeMillis();
        List<ReplicatorEvent> events = message.getEvents();
        List<Future<?>> futures = message.getFutures();
        try {
            for (ReplicatorEvent event : events) {
                futures.add(send(event));
            }
        } catch (Exception e) {
            futures.add(new FailedFuture(e));
        }
        sendDuration = System.currentTimeMillis() - start;
        int size;
        if ((size = events.size()) != 0) {
            total += size;
            metricStore.write(metricRootPath, ByteUtil.longsToBytes(total));
        }
    }

    private Future<RecordMetadata> send(ReplicatorEvent event) {
        Map<String, String> topicMap = kafkaProperties.getTableTopicMap();
        String name = event.getMetadata().getSchema() + "." + event.getMetadata().getTable();
        String topic = topicMap.getOrDefault(name, kafkaProperties.getTopic());
        LOGGER.trace("Sending event [] to topic[{}], key[{}].", event, topic);
        return kafkaProducer.send(new ProducerRecord<>(topic, event));
    }

    @Override
    protected void doStart() {
        byte[] bytes = metricStore.read(metricRootPath);
        if (ArrayUtils.isNotEmpty(bytes)) {
            total = ByteUtil.bytesToLongs(bytes)[0];
        }
    }

    @Override
    protected void doStop() {
        try {
            kafkaProducer.close(15, TimeUnit.SECONDS);
        } catch (Throwable e) {
            LOGGER.error("Failed to stop producer.", e);
        }
        metricStore.write(metricRootPath, ByteUtil.longsToBytes(total));
    }

    @Override
    public void collectMetrics(List<Collector.MetricFamilySamples> mfs) {
        CounterMetricFamily totalCounter = new CounterMetricFamily(EVENT_PROCESSED_TOTAL, PROCESSED_TOTAL_HELP, LABEL_NAMES);
        totalCounter.addMetric(labelValues, total);
        GaugeMetricFamily durationGauge = new GaugeMetricFamily(PROCESS_DURATION, PROCESS_DURATION_HELP, PROCESS_DURATION_LABEL_NAMES);
        durationGauge.addMetric(sendDurationLabelValues, sendDuration);
        mfs.add(totalCounter);
        mfs.add(durationGauge);
    }
}
