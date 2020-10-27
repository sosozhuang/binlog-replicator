package mysql.binlog.replicator.metric;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import mysql.binlog.replicator.util.AbstractLifeCycle;
import mysql.binlog.replicator.util.AppendOnlyList;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/9/14.
 */
public class EventMetricCollector extends AbstractLifeCycle {
    private static final CollectorRegistry REGISTRY = CollectorRegistry.defaultRegistry;
    private final Map<String, InternalCollector> collectors;

    public EventMetricCollector() {
        this.collectors = new HashMap<>(4);
    }

    public void addMetricHolders(String destination, MetricHolder... holders) {
        InternalCollector collector = collectors.get(destination);
        if (collector == null) {
            collector = new InternalCollector();
            collectors.put(destination, collector);
            REGISTRY.register(collector);
        }
        collector.addMetricHolders(holders);
    }

    public void removeMetricHolders(String destination) {
        InternalCollector collector = collectors.remove(destination);
        if (collector != null) {
            REGISTRY.unregister(collector);
        }
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
        collectors.values().forEach(REGISTRY::unregister);
    }

    private static class InternalCollector extends Collector {
        private final List<MetricHolder> holders;

        private InternalCollector() {
            this.holders = new LinkedList<>();
        }

        private void addMetricHolders(MetricHolder... holders) {
            for (MetricHolder holder : holders) {
                if (!this.holders.contains(holder)) {
                    this.holders.add(holder);
                }
            }
        }

        @Override
        public List<MetricFamilySamples> collect() {
            List<MetricFamilySamples> mfs = new AppendOnlyList<>();
            for (MetricHolder holder : holders) {
                holder.collectMetrics(mfs);
            }
            return mfs;
        }
    }
}
