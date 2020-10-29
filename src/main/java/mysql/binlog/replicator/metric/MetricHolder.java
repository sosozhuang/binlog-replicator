package mysql.binlog.replicator.metric;

import io.prometheus.client.Collector;

import java.util.List;

/**
 * Interface of class that holds metrics.
 *
 * @author zhuangshuo
 */
public interface MetricHolder {
    /**
     * Collect metrics to the given list.
     *
     * @param mfs the {@link io.prometheus.client.Collector.MetricFamilySamples} list
     */
    void collectMetrics(List<Collector.MetricFamilySamples> mfs);
}
