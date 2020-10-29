package mysql.binlog.replicator.model;

import com.alibaba.otter.canal.protocol.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * @author zhuangshuo
 */
public class ReplicatorMessage {
    private Message canalMessage;
    private final List<ReplicatorEvent> events;
    private final List<Future<?>> futures;

    public ReplicatorMessage() {
        this.events = new ArrayList<>(64);
        this.futures = new ArrayList<>(64);
    }

    public Message getCanalMessage() {
        return canalMessage;
    }

    public void setCanalMessage(Message canalMessage) {
        this.canalMessage = canalMessage;
    }

    public List<ReplicatorEvent> getEvents() {
        return events;
    }


    public List<Future<?>> getFutures() {
        return futures;
    }

    public void reset() {
        canalMessage = null;
        events.clear();
        futures.clear();
    }

    @Override
    public String toString() {
        return "ReplicatorMessage{" +
                ", events=" + events +
                '}';
    }
}