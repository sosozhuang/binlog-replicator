package mysql.binlog.replicator.channel;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.lmax.disruptor.WorkHandler;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import mysql.binlog.replicator.concurrent.FailedFuture;
import mysql.binlog.replicator.metric.MetricHolder;
import mysql.binlog.replicator.model.ReplicatorEvent;
import mysql.binlog.replicator.model.ReplicatorMessage;
import mysql.binlog.replicator.sink.Sink;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A WorkHandler used to convert {@link com.alibaba.otter.canal.protocol.CanalEntry} and process {@link ReplicatorEvent}s from ring buffer.
 *
 * @author zhuangshuo
 */
public class MessageHandler implements WorkHandler<ReplicatorMessage>, MetricHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandler.class);
    private static final String HOST_ADDRESS = getHostInetAddress().getHostAddress();
    private static final List<String> LABEL_NAMES = Arrays.asList("collector", "type", "destination");
    private static final String PROCESS_DURATION = "process_duration";
    private static final String PROCESS_DURATION_HELP = "Process convert duration in milliseconds.";
    private static int counter = 0;

    private final String destination;
    private final Sink<ReplicatorMessage> sink;
    private final List<String> handleDurationLabelValues;
    private volatile long handleDuration;

    MessageHandler(ClientIdentity clientId, Sink<ReplicatorMessage> sink) {
        this.destination = clientId.getDestination();
        this.sink = Objects.requireNonNull(sink);
        this.handleDuration = 0;
        String collector = "message_handler_" + counter;
        counter++;
        this.handleDurationLabelValues = Arrays.asList(collector, "handle", clientId.getDestination());
    }

    private static InetAddress getHostInetAddress() {
        Pattern pattern = Pattern.compile("[0-9]{1,3}(\\.[0-9]{1,3}){3,}");
        InetAddress addr = null;
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            if (interfaces != null) {
                while (interfaces.hasMoreElements()) {
                    NetworkInterface nic = interfaces.nextElement();
                    Enumeration<InetAddress> addrs = nic.getInetAddresses();
                    while (addrs.hasMoreElements()) {
                        addr = addrs.nextElement();
                        if (addr != null && !addr.isLoopbackAddress() && pattern.matcher(addr.getHostAddress()).matches()
                                && !"127.0.0.1".equals(addr.getHostAddress()) && !"0.0.0.0".equals(addr.getHostAddress())) {
                            return addr;
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to get network interfaces.", e);
            try {
                addr = InetAddress.getLocalHost();
            } catch (UnknownHostException inner) {
                inner.addSuppressed(e);
                LOGGER.error("Failed to get localhost address.", inner);
            }
        }
        return addr;
    }

    @Override
    public void onEvent(ReplicatorMessage message) throws Exception {
        try {
            handle(message);
        } catch (Exception e) {
            message.getFutures().add(new FailedFuture(e));
            return;
        }
        if (!CollectionUtils.isEmpty(message.getEvents())) {
            sink.process(message);
        }
    }

    private void handle(ReplicatorMessage replicatorMessage) throws Exception {
        long start = System.currentTimeMillis();
        Message message = replicatorMessage.getCanalMessage();
        if (message.isRaw()) {
            List<ByteString> rawEntries = message.getRawEntries();
            CanalEntry.Entry entry;
            for (ByteString rawEntry : rawEntries) {
                entry = CanalEntry.Entry.parseFrom(rawEntry);
                if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                    handle0(replicatorMessage, entry);
                }
            }
        } else {
            List<CanalEntry.Entry> entries = message.getEntries();
            for (CanalEntry.Entry entry : entries) {
                if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                    handle0(replicatorMessage, entry);
                }
            }
        }
        handleDuration = System.currentTimeMillis() - start;
    }

    private void handle0(ReplicatorMessage message, CanalEntry.Entry entry) throws InvalidProtocolBufferException {
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        CanalEntry.EventType eventType = rowChange.getEventType();
        if (eventType != CanalEntry.EventType.QUERY && !rowChange.getIsDdl()) {
            CanalEntry.Header entryHeader = entry.getHeader();
            List<CanalEntry.RowData> dataList = rowChange.getRowDatasList();
            LOGGER.trace("Handling row data of schema: [{}.{}], type：[{}], size: [{}].",
                    entryHeader.getSchemaName(), entryHeader.getTableName(), entryHeader.getEventType(), dataList.size());
            List<ReplicatorEvent> events = message.getEvents();
            if (eventType == CanalEntry.EventType.INSERT) {
                for (CanalEntry.RowData rowData : dataList) {
                    events.add(handleInsert(entryHeader, rowData));
                }
            } else if (eventType == CanalEntry.EventType.UPDATE) {
                for (CanalEntry.RowData rowData : dataList) {
                    events.add(handleUpdate(entryHeader, rowData));
                }
            } else if (eventType == CanalEntry.EventType.DELETE) {
                for (CanalEntry.RowData rowData : dataList) {
                    events.add(handleDelete(entryHeader, rowData));
                }
            }
        }
    }

    private ReplicatorEvent handleInsert(CanalEntry.Header entryHeader, CanalEntry.RowData rowData) {
        ReplicatorEvent event = ReplicatorEvent.newInsertEvent();
        List<CanalEntry.Column> cols = rowData.getAfterColumnsList();
        String timestamp = null, name, value;
        for (CanalEntry.Column col : cols) {
            name = col.getName();
            value = col.getValue();
            if ("update_time".equalsIgnoreCase(name)) {
                timestamp = col.getValue();
            }
            event.addAfter(name, value);
        }
        if (StringUtils.isBlank(timestamp)) {
            timestamp = String.valueOf(entryHeader.getExecuteTime());
        }
        ReplicatorEvent.Metadata metadata = event.getMetadata();
        metadata.setDestination(destination);
        metadata.setSchema(entryHeader.getSchemaName());
        metadata.setTable(entryHeader.getTableName());
        metadata.setEventType("insert");
        metadata.setReplicator(HOST_ADDRESS);
        metadata.setTimestamp(timestamp);
        return event;
    }

    private ReplicatorEvent handleUpdate(CanalEntry.Header entryHeader, CanalEntry.RowData rowData) {
        ReplicatorEvent event = ReplicatorEvent.newUpdateEvent();
        List<CanalEntry.Column> cols = rowData.getBeforeColumnsList();
        String timestamp = null, name, value;
        for (CanalEntry.Column col : cols) {
            name = col.getName();
            value = col.getValue();
            event.addBefore(name, value);
        }
        cols = rowData.getAfterColumnsList();
        for (CanalEntry.Column col : cols) {
            name = col.getName();
            value = col.getValue();
            if ("update_time".equalsIgnoreCase(name)) {
                timestamp = col.getValue();
            }
            event.addAfter(name, value);
        }
        if (StringUtils.isBlank(timestamp)) {
            timestamp = String.valueOf(entryHeader.getExecuteTime());
        }
        ReplicatorEvent.Metadata metadata = event.getMetadata();
        metadata.setDestination(destination);
        metadata.setSchema(entryHeader.getSchemaName());
        metadata.setTable(entryHeader.getTableName());
        metadata.setEventType("update");
        metadata.setReplicator(HOST_ADDRESS);
        metadata.setTimestamp(timestamp);
        return event;
    }

    private ReplicatorEvent handleDelete(CanalEntry.Header entryHeader, CanalEntry.RowData rowData) {
        ReplicatorEvent event = ReplicatorEvent.newDeleteEvent();
        List<CanalEntry.Column> cols = rowData.getBeforeColumnsList();
        String timestamp = null, name, value;
        for (CanalEntry.Column col : cols) {
            name = col.getName();
            value = col.getValue();
            if ("update_time".equalsIgnoreCase(name)) {
                timestamp = col.getValue();
            }
            event.addBefore(name, value);
        }
        if (StringUtils.isBlank(timestamp)) {
            timestamp = String.valueOf(entryHeader.getExecuteTime());
        }
        ReplicatorEvent.Metadata metadata = event.getMetadata();
        metadata.setDestination(destination);
        metadata.setSchema(entryHeader.getSchemaName());
        metadata.setTable(entryHeader.getTableName());
        metadata.setEventType("update");
        metadata.setReplicator(HOST_ADDRESS);
        metadata.setTimestamp(timestamp);
        return event;
    }

    @Override
    public void collectMetrics(List<Collector.MetricFamilySamples> mfs) {
        GaugeMetricFamily handleDurationGauge = new GaugeMetricFamily(PROCESS_DURATION, PROCESS_DURATION_HELP, LABEL_NAMES);
        handleDurationGauge.addMetric(handleDurationLabelValues, handleDuration);
        mfs.add(handleDurationGauge);
    }
}
