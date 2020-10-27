package mysql.binlog.replicator.channel;

import com.alibaba.otter.canal.common.CanalException;
import com.lmax.disruptor.EventHandler;
import mysql.binlog.replicator.model.ReplicatorMessage;
import mysql.binlog.replicator.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;

/**
 * Waiting for all futures of {@link ReplicatorMessage} to be completed.
 *
 * @author zhuangshuo
 */
public class FutureHandler implements EventHandler<ReplicatorMessage> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FutureHandler.class);
    private final Source source;
    private boolean skip;

    FutureHandler(Source source) {
        this.source = Objects.requireNonNull(source);
        this.skip = false;
    }

    @Override
    public void onEvent(ReplicatorMessage message, long sequence, boolean endOfBatch) throws Exception {
        try {
            long id = message.getCanalMessage().getId();
            if (!skip) {
                List<Future<?>> futures = message.getFutures();
                try {
                    for (Future<?> future : futures) {
                        future.get();
                    }
                    source.commit(id);
                } catch (CanalException e) {
                    LOGGER.error("Failed to commit message, id = [{}].", id, e);
                    rollback();
                    tryResume(id);
                } catch (Exception e) {
                    LOGGER.error("Future execution failed, id = [{}].", id, e);
                    rollback();
                    tryResume(id);
                }
            } else {
                tryResume(id);
            }
        } finally {
            message.reset();
        }
    }

    private void rollback() throws InterruptedException {
        skip = true;
        source.rollback();
    }

    private void tryResume(long id) {
        if (id == source.getLastFetchId()) {
            LOGGER.info("End of fetched messages, id = [{}], setting skip to false.", id);
            skip = false;
            source.resume();
        }
    }
}
