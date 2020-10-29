package mysql.binlog.replicator.channel;

import com.alibaba.otter.canal.protocol.Message;
import com.lmax.disruptor.EventTranslatorOneArg;
import mysql.binlog.replicator.model.ReplicatorMessage;

/**
 * Set {@link Message} to {@link ReplicatorMessage}.
 *
 * @author zhuangshuo
 */
public class MessageTranslator implements EventTranslatorOneArg<ReplicatorMessage, Message> {
    @Override
    public void translateTo(ReplicatorMessage replicatorMessage, long seq, Message message) {
        replicatorMessage.setCanalMessage(message);
    }
}
