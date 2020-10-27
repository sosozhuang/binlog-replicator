package mysql.binlog.replicator.concurrent;

import org.apache.commons.lang3.Validate;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/3/28.
 */
public class SimpleThreadFactory implements ThreadFactory {
    private final String prefix;
    private final boolean daemon;
    private final AtomicInteger counter;

    public SimpleThreadFactory(String prefix) {
        this(prefix, false);
    }

    public SimpleThreadFactory(String prefix, boolean daemon) {
        this.prefix = Validate.notBlank(prefix);
        this.daemon = daemon;
        this.counter = new AtomicInteger();
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, prefix + "-" + counter.incrementAndGet());
        t.setDaemon(daemon);
        return t;
    }
}
