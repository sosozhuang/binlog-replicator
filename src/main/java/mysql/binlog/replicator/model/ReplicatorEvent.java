package mysql.binlog.replicator.model;

import java.util.*;

/**
 * @author zhuangshuo
 */
public class ReplicatorEvent {
    private List<Map.Entry<String, String>> before;
    private List<Map.Entry<String, String>> after;
    private final Metadata metadata;

    private ReplicatorEvent() {
        this.metadata = new Metadata();
    }

    public static ReplicatorEvent newEvent() {
        return newUpdateEvent();
    }

    public static ReplicatorEvent newInsertEvent() {
        ReplicatorEvent event = new ReplicatorEvent();
        event.before = Collections.emptyList();
        event.after = new ArrayList<>(32);
        return event;
    }

    public static ReplicatorEvent newUpdateEvent() {
        ReplicatorEvent event = new ReplicatorEvent();
        event.before = new ArrayList<>(32);
        event.after = new ArrayList<>(32);
        return event;
    }

    public static ReplicatorEvent newDeleteEvent() {
        ReplicatorEvent event = new ReplicatorEvent();
        event.before = new ArrayList<>(32);
        event.after = Collections.emptyList();
        return event;
    }

    public void addBefore(String key, String value) {
        before.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
    }

    public void addAfter(String key, String value) {
        after.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
    }

    public List<Map.Entry<String, String>> getBefore() {
        return before;
    }

    public List<Map.Entry<String, String>> getAfter() {
        return after;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "ReplicatorEvent{" +
                "before=" + before +
                ", after=" + after +
                ", metadata=" + metadata +
                '}';
    }

    public static class Metadata {
        private String destination;
        private String schema;
        private String table;
        private String eventType;
        private String replicator;
        private long timestamp;

        public String getDestination() {
            return destination;
        }

        public Metadata setDestination(String destination) {
            this.destination = destination;
            return this;
        }

        public String getSchema() {
            return schema;
        }

        public Metadata setSchema(String schema) {
            this.schema = schema;
            return this;
        }

        public String getTable() {
            return table;
        }

        public Metadata setTable(String table) {
            this.table = table;
            return this;
        }

        public String getEventType() {
            return eventType;
        }

        public Metadata setEventType(String eventType) {
            this.eventType = eventType;
            return this;
        }

        public String getReplicator() {
            return replicator;
        }

        public Metadata setReplicator(String replicator) {
            this.replicator = replicator;
            return this;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public Metadata setTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        @Override
        public String toString() {
            return "Metadata{" +
                    "destination='" + destination + '\'' +
                    ", schema='" + schema + '\'' +
                    ", table='" + table + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", replicator='" + replicator + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
