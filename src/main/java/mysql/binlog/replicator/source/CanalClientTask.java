package mysql.binlog.replicator.source;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.CanalService;
import mysql.binlog.replicator.channel.Channel;
import mysql.binlog.replicator.channel.DisruptorChannel;
import mysql.binlog.replicator.metric.EventMetricCollector;
import mysql.binlog.replicator.metric.MetricStore;
import mysql.binlog.replicator.util.AbstractLifeCycle;
import mysql.binlog.replicator.util.Configuration;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * @author zhuangshuo
 */
public class CanalClientTask extends AbstractLifeCycle implements Source, Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalClientTask.class);
    private static final int DEFAULT_BATCH_SIZE = 1024;
    private final String destination;
    private final EventMetricCollector collector;
    private final CanalClient canalClient;
    private final Channel<Message> channel;
    private final AtomicBoolean shouldStop;
    private final Semaphore permit;
    private final AtomicBoolean shouldWait;
    private final Semaphore waitSemaphore;
    private int batchSize;

    public CanalClientTask(Configuration config,
                           CanalService canalService,
                           String destination,
                           MetricStore metricStore,
                           EventMetricCollector collector) {
        this.destination = Validate.notBlank(destination, "destination cannot be empty string");
        this.collector = Objects.requireNonNull(collector);
        ClientIdentity clientId = new ClientIdentity(this.destination, (short) 1001);
        this.canalClient = new CanalClient(canalService, clientId, metricStore);
        this.shouldStop = new AtomicBoolean(false);
        this.permit = new Semaphore(1);
        this.shouldWait = new AtomicBoolean(false);
        this.waitSemaphore = new Semaphore(1);
        this.batchSize = config.getInteger("replicator.canal.batchSize", DEFAULT_BATCH_SIZE);

        this.collector.addMetricHolders(this.destination, canalClient);
        this.channel = new DisruptorChannel(config, clientId, this, metricStore, collector);
    }

    @Override
    protected void doStart() {
        channel.start();
        canalClient.start();
    }

    @Override
    public void run() {
        MDC.put("destination", destination);
        try {
            waitSemaphore.acquire();
            permit.acquire();
            LOGGER.debug("CanalClientTask start to process.");
            Message msg;
            while (!shouldStop.get()) {
                try {
                    msg = canalClient.fetch(batchSize);
                    if (msg != null) {
                        channel.publish(msg);
                    }
                } catch (Exception e) {
                    LOGGER.error("Exception occurs when fetching messages.", e);
                }

                while (shouldWait.get()) {
                    LOGGER.warn("CanalClientTask paused, id = [{}].", getLastFetchId());
                    waitSemaphore.release();
                    LockSupport.parkNanos(500);
                    waitSemaphore.acquire();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("CanalClientTask interrupted while acquire permit.", e);
        } finally {
            waitSemaphore.release();
            permit.release();
        }
    }

    @Override
    protected void doStop() {
        shouldStop.set(true);
        try {
            permit.acquire();
            canalClient.stop();
            channel.stop();
            collector.removeMetricHolders(destination);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("CanalClientTask interrupted while acquire permit.", e);
        } finally {
            permit.release();
        }
    }

    @Override
    public void commit(long id) {
        canalClient.commit(id);
    }

    @Override
    public void rollback(long id) {
        shouldWait.set(true);
        waitSemaphore.acquireUninterruptibly();
        canalClient.rollback(id);
    }

    @Override
    public void rollback() {
        shouldWait.set(true);
        waitSemaphore.acquireUninterruptibly();
        canalClient.rollback();
    }

    @Override
    public void resume() {
        shouldWait.set(false);
        waitSemaphore.release();
        LOGGER.info("CanalClientTask resumed, id = [{}].", getLastFetchId());
    }

    @Override
    public long getLastFetchId() {
        return canalClient.getLastFetchId();
    }
}
