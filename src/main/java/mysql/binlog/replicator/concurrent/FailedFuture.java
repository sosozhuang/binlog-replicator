package mysql.binlog.replicator.concurrent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A Future is already failed.
 *
 * @author zhuangshuo
 */
public class FailedFuture implements Future<Void> {
    private final ExecutionException e;

    public FailedFuture(Exception e) {
        this.e = new ExecutionException(e);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        throw e;
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throw e;
    }
}
