package mysql.binlog.replicator.source;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.CanalService;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;
import mysql.binlog.replicator.metric.MetricHolder;
import mysql.binlog.replicator.metric.MetricStore;
import mysql.binlog.replicator.util.AbstractLifeCycle;
import mysql.binlog.replicator.util.ByteUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author zhuangshuo
 */
public class CanalClient extends AbstractLifeCycle implements MetricHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalClient.class);
    private static final String LAST_BATCH_ID = "last_batch_id";
    private static final String LAST_BATCH_ID_HELP = "Last message id in source client.";
    private static final List<String> LAST_BATCH_ID_LABEL_NAMES = Arrays.asList("collector", "step", "destination");
    private static final String MESSAGES_FETCHED_TOTAL = "messages_fetched_total";
    private static final String ENTRIES_FETCHED_TOTAL = "entries_fetched_total";
    private static final String FETCHED_TOTAL_HELP = "Total number of fetched messages/entries from canal server.";
    private static final List<String> LABEL_NAMES = Arrays.asList("collector", "type", "destination");
    private static final String PROCESS_DURATION = "process_duration";
    private static final String PROCESS_DURATION_HELP = "Process duration in milliseconds.";

    private final ClientIdentity clientId;
    private final CanalService canalService;
    private final MetricStore metricStore;
    private final String metricRootPath;
    private final List<String> fetchIdLabelValues;
    private final List<String> ackIdLabelValues;
    private final List<String> rollbackIdLabelValues;
    private final List<String> messageLabelValues;
    private final List<String> entryLabelValues;
    private final List<String> fetchDurationLabelValues;
    private volatile long lastFetchId;
    private volatile long lastAckId;
    private volatile long lastRollbackId;
    private volatile long messageTotal;
    private volatile long entryTotal;
    private volatile long fetchDuration;

    CanalClient(CanalService canalService, ClientIdentity clientId, MetricStore metricStore) {
        this.canalService = Objects.requireNonNull(canalService);
        this.clientId = clientId;
        this.metricStore = Objects.requireNonNull(metricStore);
        this.metricRootPath = MetricStore.getPath(clientId, "canal_client");
        this.fetchIdLabelValues = Arrays.asList("canal_client", "fetch", clientId.getDestination());
        this.ackIdLabelValues = Arrays.asList("canal_client", "ack", clientId.getDestination());
        this.rollbackIdLabelValues = Arrays.asList("canal_client", "rollback", clientId.getDestination());
        this.messageLabelValues = Arrays.asList("canal_client", "message", clientId.getDestination());
        this.entryLabelValues = Arrays.asList("canal_client", "entry", clientId.getDestination());
        this.fetchDurationLabelValues = Arrays.asList("canal_client", "fetch", clientId.getDestination());
        this.lastFetchId = 0;
        this.lastAckId = 0;
        this.lastRollbackId = 0;
        this.messageTotal = 0;
        this.entryTotal = 0;
        this.fetchDuration = 0;
    }

    Message fetch(int batchSize) {
        long start = System.currentTimeMillis();
        Message message = canalService.getWithoutAck(clientId, batchSize);
        fetchDuration = System.currentTimeMillis() - start;
        long batchId = message.getId();
        int size = message.isRaw() ? message.getRawEntries().size() : message.getEntries().size();
        if (batchId == -1 || size == 0) {
            return null;
        }
        LOGGER.info("Message fetched, batch = [{}], size is [{}].", batchId, size);
        lastFetchId = batchId;
        messageTotal += 1;
        entryTotal += size;
        metricStore.write(metricRootPath, ByteUtil.longsToBytes(messageTotal, entryTotal));
        return message;
    }

    void commit(long batchId) {
        canalService.ack(clientId, batchId);
        lastAckId = batchId;
        LOGGER.debug("Message committed, id = [{}].", batchId);
    }

    void rollback() {
        canalService.rollback(clientId);
        lastRollbackId = getLastFetchId();
        LOGGER.warn("Rollback all messages, id = [{}].", lastRollbackId);
    }

    void rollback(long batchId) {
        canalService.rollback(clientId, batchId);
        lastRollbackId = batchId;
        LOGGER.warn("Rollback message, id = [{}].", batchId);
    }

    long getLastFetchId() {
        return lastFetchId;
    }

    @Override
    protected void doStart() {
        byte[] bytes = metricStore.read(metricRootPath);
        if (ArrayUtils.isNotEmpty(bytes)) {
            long[] values = ByteUtil.bytesToLongs(bytes);
            messageTotal = values[0];
            entryTotal = values[1];
        }
        canalService.subscribe(clientId);
    }

    @Override
    protected void doStop() {
        metricStore.write(metricRootPath, ByteUtil.longsToBytes(messageTotal, entryTotal));
    }

    @Override
    public void collectMetrics(List<Collector.MetricFamilySamples> mfs) {
        GaugeMetricFamily batchIdGauge = new GaugeMetricFamily(LAST_BATCH_ID, LAST_BATCH_ID_HELP, LAST_BATCH_ID_LABEL_NAMES);
        batchIdGauge.addMetric(fetchIdLabelValues, lastFetchId);
        batchIdGauge.addMetric(ackIdLabelValues, lastAckId);
        batchIdGauge.addMetric(rollbackIdLabelValues, lastRollbackId);
        CounterMetricFamily messageCounter = new CounterMetricFamily(MESSAGES_FETCHED_TOTAL, FETCHED_TOTAL_HELP, LABEL_NAMES);
        messageCounter.addMetric(messageLabelValues, messageTotal);
        CounterMetricFamily entryCounter = new CounterMetricFamily(ENTRIES_FETCHED_TOTAL, FETCHED_TOTAL_HELP, LABEL_NAMES);
        entryCounter.addMetric(entryLabelValues, entryTotal);
        GaugeMetricFamily durationGauge = new GaugeMetricFamily(PROCESS_DURATION, PROCESS_DURATION_HELP, LABEL_NAMES);
        durationGauge.addMetric(fetchDurationLabelValues, fetchDuration);
        mfs.add(batchIdGauge);
        mfs.add(messageCounter);
        mfs.add(entryCounter);
        mfs.add(durationGauge);
    }
}
