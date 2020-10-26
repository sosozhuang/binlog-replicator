package mysql.binlog.replicator.channel;

import com.lmax.disruptor.EventFactory;
import mysql.binlog.replicator.model.ChannelMessage;

/**
 * An {@link EventFactory} that creates {@link ChannelMessage}.
 *
 * @author zhuangshuo
 */
public class MessageEventFactory implements EventFactory<ChannelMessage> {
    @Override
    public ChannelMessage newInstance() {
        return new ChannelMessage();
    }
}
