package mysql.binlog.replicator.channel;

import com.lmax.disruptor.EventFactory;
import mysql.binlog.replicator.model.ReplicatorMessage;

/**
 * An {@link EventFactory} that creates {@link ReplicatorMessage}.
 *
 * @author zhuangshuo
 */
public class MessageEventFactory implements EventFactory<ReplicatorMessage> {
    @Override
    public ReplicatorMessage newInstance() {
        return new ReplicatorMessage();
    }
}
