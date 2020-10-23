package mysql.binlog.replicate.sink;

import mysql.binlog.replicate.util.LifeCycle;

/**
 * Process data from {@link mysql.binlog.replicate.channel.Channel}.
 *
 * @param <T> the type of data to process
 * @author zhuangshuo
 */
public interface Sink<T> extends LifeCycle {
    /**
     * Process the given data.
     *
     * @param data the data to process
     */
    void process(T data);
}
