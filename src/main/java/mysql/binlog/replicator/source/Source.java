package mysql.binlog.replicator.source;

/**
 * @author zhuangshuo
 */
public interface Source {
    void commit(long id);

    void rollback(long id);

    void rollback();

    void resume();

    long getLastFetchId();
}
