package mysql.binlog.replicator.util;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhuangshuo
 */
public abstract class AbstractLifeCycle implements LifeCycle {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLifeCycle.class);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final String name;

    protected AbstractLifeCycle() {
        this.name = this.getClass().getSimpleName();
    }

    protected AbstractLifeCycle(int i) {
        this(String.valueOf(i));
    }

    protected AbstractLifeCycle(String name) {
        this.name = this.getClass().getSimpleName() + "_" + Validate.notBlank(name, "name cannot be empty string");
    }

    private boolean checkAlreadyStarted() {
        return started.compareAndSet(true, false);
    }

    private boolean checkNotStarted() {
        return started.compareAndSet(false, true);
    }

    protected abstract void doStart();

    protected abstract void doStop();

    @Override
    public void start() {
        if (!checkNotStarted()) {
            return;
        }
        doStart();
        LOGGER.info("{} started.", name);
    }

    @Override
    public void stop() {
        if (!checkAlreadyStarted()) {
            return;
        }
        doStop();
        LOGGER.info("{} stopped.", name);
    }

    @Override
    public boolean isStarted() {
        return started.get();
    }
}
