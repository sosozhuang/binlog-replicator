package mysql.binlog.replicator.channel;

import com.alibaba.otter.canal.protocol.Message;
import com.lmax.disruptor.EventTranslatorOneArg;
import mysql.binlog.replicator.model.ChannelMessage;

/**
 * Set {@link Message} to {@link ChannelMessage}.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/4/3.
 */
public class MessageTranslator implements EventTranslatorOneArg<ChannelMessage, Message> {
    @Override
    public void translateTo(ChannelMessage channelMessage, long seq, Message message) {
        channelMessage.setCanalMessage(message);
    }
}
