package mysql.binlog.replicator.metric;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.commons.lang3.Validate;

import static com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils.ZOOKEEPER_SEPARATOR;

/**
 * Stores metrics in zookeeper.
 *
 * @author zhuangshuo
 * Created by zhuangshuo on 2020/9/19.
 */
public class MetricStore {
    private final ZkClientx zkClientx;

    public MetricStore(String zkServers) {
        this.zkClientx = ZkClientx.getZkClient(Validate.notBlank(zkServers, "zkServers cannot be empty string"));
    }

    public byte[] read(String path) {
        Validate.notBlank(path, "path cannot be empty string");
        return zkClientx.readData(path, true);
    }

    public void write(String path, byte[] value) {
        Validate.notBlank(path, "path cannot be empty string");
        try {
            zkClientx.writeData(path, value);
        } catch (ZkNoNodeException e) {
            zkClientx.createPersistent(path, value, true);
        }
    }

    public static String getPath(ClientIdentity clientIdentity, String path) {
        return ZookeeperPathUtils.getClientIdNodePath(clientIdentity.getDestination(), clientIdentity.getClientId()) + ZOOKEEPER_SEPARATOR + path;
    }
}
