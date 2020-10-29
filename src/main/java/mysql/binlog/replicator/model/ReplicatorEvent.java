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
        private String timestamp;

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        public String getReplicator() {
            return replicator;
        }

        public void setReplicator(String replicator) {
            this.replicator = replicator;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(String timestamp) {
            this.timestamp = timestamp;
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
