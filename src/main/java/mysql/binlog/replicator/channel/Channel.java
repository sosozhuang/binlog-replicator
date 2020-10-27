package mysql.binlog.replicator.channel;

import mysql.binlog.replicator.util.LifeCycle;

/**
 * Designed for holding message prior to processing.
 *
 * @author zhuangshuo
 */
public interface Channel<T> extends LifeCycle {
    /**
     * Publish the given message.
     *
     * @param message the message to publish
     */
    void publish(T message);
}
