package mysql.binlog.replicator.sink;

import mysql.binlog.replicator.util.LifeCycle;

/**
 * Process messages from {@link mysql.binlog.replicator.channel.Channel}.
 *
 * @param <T> the type of message to process
 * @author zhuangshuo
 */
public interface Sink<T> extends LifeCycle {
    /**
     * Process the given message.
     *
     * @param message the message to process
     */
    void process(T message);
}
