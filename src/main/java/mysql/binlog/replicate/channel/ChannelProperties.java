package mysql.binlog.replicate.channel;

import com.lmax.disruptor.*;
import mysql.binlog.replicate.util.Configuration;

/**
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/3/5.
 */
public class ChannelProperties {
    private static final int DEFAULT_RING_BUFFER_SIZE = 2048;
    private static final String DEFAULT_WAIT_STRATEGY = "BLOCKING";
    private static final int DEFAULT_ENTRY_WORKER_THREADS = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
    private final int entryBufferSize;
    private final int entryWorkerThreads;
    private final WaitStrategy waitStrategy;

    public ChannelProperties(Configuration config) {
        this.entryBufferSize = config.getInteger("channel.entry.ringBuffer.size", DEFAULT_RING_BUFFER_SIZE);
        this.entryWorkerThreads = config.getInteger("channel.entry.workerThreads", DEFAULT_ENTRY_WORKER_THREADS);
        this.waitStrategy = getWaitStrategyFromString(config.getString("channel.waitStrategy", DEFAULT_WAIT_STRATEGY));
    }

    private static WaitStrategy getWaitStrategyFromString(String s) {
        switch (s.trim().toUpperCase()) {
            case "BLOCKING":
                return new BlockingWaitStrategy();
            case "BUSY_SPIN":
                return new BusySpinWaitStrategy();
            case "LITE_BLOCKING":
                return new LiteBlockingWaitStrategy();
            case "SLEEPING":
                return new SleepingWaitStrategy();
            case "YIELDING":
                return new YieldingWaitStrategy();
            default:
                throw new IllegalArgumentException();
        }
    }

    public int getEntryBufferSize() {
        return entryBufferSize;
    }

    public int getEntryWorkerThreads() {
        return entryWorkerThreads;
    }

    public WaitStrategy getWaitStrategy() {
        return waitStrategy;
    }

    @Override
    public String toString() {
        return "ChannelProperties{" +
                "entryBufferSize=" + entryBufferSize +
                ", entryWorkerThreads=" + entryWorkerThreads +
                ", waitStrategy=" + waitStrategy +
                '}';
    }
}
