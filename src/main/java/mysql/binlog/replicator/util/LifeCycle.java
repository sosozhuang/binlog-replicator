package mysql.binlog.replicator.util;

/**
 * A common interface defining methods for start/stop lifecycle control.
 *
 * @author zhuangshuo
 */
public interface LifeCycle {
    /**
     * Start the component.
     */
    void start();

    /**
     * Stop the component.
     */
    void stop();

    /**
     * @return true if started, false otherwise.
     */
    boolean isStarted();
}
