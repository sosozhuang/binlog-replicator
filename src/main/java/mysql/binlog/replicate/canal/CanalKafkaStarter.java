package mysql.binlog.replicate.canal;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.server.CanalMQStarter;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import mysql.binlog.replicate.metric.EventMetricCollector;
import mysql.binlog.replicate.metric.MetricStore;
import mysql.binlog.replicate.util.Configuration;
import mysql.binlog.replicate.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Start with {@link com.alibaba.otter.canal.deployer.CanalController}.
 *
 * @author zhuangshuo
 * Created by T_zhuangshuo_kzx on 2020/9/26.
 */
public class CanalKafkaStarter extends CanalMQStarter implements LifeCycle {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalKafkaStarter.class);
    private final Configuration config;
    private final AtomicBoolean started;
    private final ExecutorService executorService;
    private final MetricStore metricStore;
    private final EventMetricCollector metricCollector;
    private final CanalServerWithEmbedded canalServer;
    private final Map<String, CanalClientTask> tasks;

    public CanalKafkaStarter(Configuration config) {
        super(null);
        this.config = config;
        // disable netty
        System.setProperty("canal.withoutNetty", "true");
        System.setProperty("canal.instance.filter.transaction.entry", "true");
        this.started = new AtomicBoolean(false);
        this.executorService = new ThreadPoolExecutor(1, 16,
                30, TimeUnit.SECONDS, new SynchronousQueue<>());
        this.metricStore = new MetricStore(this.config.getString("canal.zkServers"));
        this.metricCollector = new EventMetricCollector();
        this.canalServer = CanalServerWithEmbedded.instance();
        this.tasks = new ConcurrentHashMap<>();
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        metricCollector.start();
        LOGGER.info("CanalKafkaStarter started.");
    }

    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        for (Map.Entry<String, CanalClientTask> entry : tasks.entrySet()) {
            MDC.put("destination", entry.getKey());
            try {
                entry.getValue().stop();
            } catch (Throwable e) {
                LOGGER.error("Failed to stop clientTask.", e);
            }
            MDC.remove("destination");
        }
        try {
            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Interrupted while waiting.", e);
        } catch (Throwable e) {
            LOGGER.error("Failed to stop executorService.", e);
        }
        metricCollector.stop();
        LOGGER.info("CanalKafkaStarter stopped.");
    }

    @Override
    public boolean isStarted() {
        return started.get();
    }

    @Override
    public synchronized void startDestination(String destination) {
        CanalInstance canalInstance = canalServer.getCanalInstances().get(destination);
        if (canalInstance != null) {
            stopDestination(destination);
            waitTillReady(canalInstance.getDestination());
            MDC.put("destination", canalInstance.getDestination());
            CanalClientTask task = new CanalClientTask(config, canalServer,
                    canalInstance.getDestination(), metricStore, metricCollector);
            task.start();
            MDC.remove("destination");
            tasks.put(canalInstance.getDestination(), task);
            executorService.execute(task);
            LOGGER.info("CanalKafkaStarter started destination: {}.", destination);
        }
    }

    @Override
    public synchronized void stopDestination(String destination) {
        CanalClientTask task = tasks.get(destination);
        if (task != null) {
            MDC.put("destination", destination);
            task.stop();
            MDC.remove("destination");
            tasks.remove(destination);
            LOGGER.info("CanalKafkaStarter stopped destination: {}.", destination);
        }
    }

    private void waitTillReady(String destination) {
        while (!canalServer.getCanalInstances().containsKey(destination)) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
