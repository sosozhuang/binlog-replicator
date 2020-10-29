package mysql.binlog.replicator;

import com.alibaba.otter.canal.deployer.CanalConstants;
import com.alibaba.otter.canal.deployer.CanalController;
import mysql.binlog.replicator.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Main class of binlog-replicator, for reading binlog events from MySQL, and transferring to Kafka.
 *
 * @author zhuangshuo
 */
public class Replicator {
    private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);
    private static final String CONFIG_ENV_PROPERTY_KEY = "replicator.config.location";

    public static void main(String[] args) {
        Configuration config = null;
        String fileName = System.getProperty(CONFIG_ENV_PROPERTY_KEY);
        try {
            if (StringUtils.isBlank(fileName)) {
                fileName = "replicator.properties";
                config = new Configuration(fileName);
            } else {
                config = new Configuration(new File(fileName));
            }
        } catch (IOException e) {
            LOGGER.error("Failed to load configuration from [{}].", fileName, e);
            System.exit(1);
        }

        String serverMode = config.getString(CanalConstants.CANAL_SERVER_MODE);
        CanalKafkaStarter canalKafkaStarter;
        if ("kafka".equalsIgnoreCase(serverMode)) {
            canalKafkaStarter = new CanalKafkaStarter(config);
        } else {
            canalKafkaStarter = null;
        }

        Properties properties = new Properties();
        properties.putAll(config.asMap());
        CanalController controller = new CanalController(properties);
        controller.setCanalMQStarter(canalKafkaStarter);
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (canalKafkaStarter != null) {
                    canalKafkaStarter.stop();
                }
                try {
                    controller.stop();
                } catch (Throwable e) {
                    LOGGER.error("Failed to stop CanalController.", e);
                }
            }));
            setDefaultUncaughtExceptionHandler();
            controller.start();
            if (canalKafkaStarter != null) {
                canalKafkaStarter.start();
            }
        } catch (Throwable e) {
            LOGGER.error("Failed to start Replicator.", e);
            System.exit(1);
        }
    }

    private static void setDefaultUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> LOGGER.error("Exception caught in {}.", t.getName(), e));
    }
}
