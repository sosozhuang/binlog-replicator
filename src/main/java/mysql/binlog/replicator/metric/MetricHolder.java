package mysql.binlog.replicator.metric;

import io.prometheus.client.Collector;

import java.util.List;

/**
 * Interface of class that holds metrics.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/3/14.
 */
public interface MetricHolder {
    /**
     * Collect metrics to the given list.
     *
     * @param mfs the {@link io.prometheus.client.Collector.MetricFamilySamples} list
     */
    void collectMetrics(List<Collector.MetricFamilySamples> mfs);
}
