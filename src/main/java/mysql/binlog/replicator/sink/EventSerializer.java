package mysql.binlog.replicator.sink;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.netty.util.CharsetUtil;
import mysql.binlog.replicator.model.ReplicatorEvent;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class EventSerializer implements Serializer<ReplicatorEvent> {
    private static final Gson GSON = new GsonBuilder().registerTypeAdapter(ReplicatorEvent.class, new EventTypeAdapter())
            .create();
    private static final Charset UTF_8 = CharsetUtil.UTF_8;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, ReplicatorEvent event) {
        return GSON.toJson(event).getBytes(UTF_8);
    }

    @Override
    public void close() {

    }

    /**
     * A TypeAdapter used to serialize and deserialize {@link ReplicatorEvent}.
     *
     * @author zhuangshuo
     */
    private static class EventTypeAdapter extends TypeAdapter<ReplicatorEvent> {
        @Override
        public void write(JsonWriter writer, ReplicatorEvent event) throws IOException {
            writer.beginObject();
            List<Map.Entry<String, String>> entries = event.getBefore();
            if (entries != null && entries.size() > 0) {
                writer.name("before").beginObject();
                for (Map.Entry<String, String> entry : entries) {
                    writer.name(entry.getKey()).value(entry.getValue());
                }
                writer.endObject();
            }
            entries = event.getAfter();
            if (entries != null && entries.size() > 0) {
                writer.name("after").beginObject();
                for (Map.Entry<String, String> entry : entries) {
                    writer.name(entry.getKey()).value(entry.getValue());
                }
                writer.endObject();
            }
            ReplicatorEvent.Metadata metadata = event.getMetadata();
            writer.name("metadata").beginObject();
            writer.name("destination").value(metadata.getDestination());
            writer.name("schema").value(metadata.getSchema());
            writer.name("table").value(metadata.getTable());
            writer.name("event_type").value(metadata.getEventType());
            writer.name("replicator").value(metadata.getReplicator());
            writer.name("timestamp").value(metadata.getTimestamp());
            writer.endObject();

            writer.endObject();
        }

        @Override
        public ReplicatorEvent read(JsonReader reader) throws IOException {
            ReplicatorEvent event = ReplicatorEvent.newEvent();
            reader.beginObject();
            String name;
            while (reader.hasNext()) {
                name = reader.nextName();
                switch (name) {
                    case "before":
                        reader.beginObject();
                        while (reader.hasNext()) {
                            event.addBefore(reader.nextName(), reader.nextString());
                        }
                        reader.endObject();
                        break;
                    case "after":
                        reader.beginObject();
                        while (reader.hasNext()) {
                            event.addAfter(reader.nextName(), reader.nextString());
                        }
                        reader.endObject();
                        break;
                    case "metadata":
                        reader.beginObject();
                        ReplicatorEvent.Metadata metadata = event.getMetadata();
                        while (reader.hasNext()) {
                            switch (reader.nextName()) {
                                case "destination":
                                    metadata.setDestination(reader.nextString());
                                    break;
                                case "schema":
                                    metadata.setSchema(reader.nextString());
                                    break;
                                case "table":
                                    metadata.setTable(reader.nextString());
                                    break;
                                case "event_type":
                                    metadata.setEventType(reader.nextString());
                                    break;
                                case "replicator":
                                    metadata.setReplicator(reader.nextString());
                                    break;
                                case "timestamp":
                                    metadata.setTimestamp(reader.nextString());
                                    break;
                            }
                        }
                        reader.endObject();
                        break;
                }
            }
            reader.endObject();
            return event;
        }
    }
}
