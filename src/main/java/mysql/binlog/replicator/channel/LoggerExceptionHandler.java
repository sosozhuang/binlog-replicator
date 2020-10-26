package mysql.binlog.replicator.channel;

import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs stack trace of exception to logger.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/4/3.
 */
public class LoggerExceptionHandler<T> implements ExceptionHandler<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerExceptionHandler.class);

    @Override
    public void handleEventException(Throwable throwable, long sequence, T event) {
        LOGGER.error("Unexpected exception caught while handling event [{}].", event, throwable);
    }

    @Override
    public void handleOnStartException(Throwable throwable) {
        LOGGER.error("Unexpected exception caught on start.", throwable);
    }

    @Override
    public void handleOnShutdownException(Throwable throwable) {
        LOGGER.error("Unexpected exception caught on shutdown.", throwable);
    }
}
