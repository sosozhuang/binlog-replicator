package mysql.binlog.replicator.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;

/**
 * @author zhuangshuo
 */
public final class ResourceLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceLoader.class);

    private ResourceLoader() {
        throw new RuntimeException();
    }

    private static ClassLoader getThreadContextLoader() throws InvocationTargetException, IllegalAccessException {
        Method method;
        try {
            method = Thread.class.getMethod("getContextClassLoader");
        } catch (NoSuchMethodException e) {
            return null;
        }
        return (ClassLoader) method.invoke(Thread.currentThread());
    }

    public static URL getResource(String resource) {
        ClassLoader classLoader;
        URL url;

        try {
            classLoader = getThreadContextLoader();
            if (classLoader != null) {
                LOGGER.debug("Trying to find [{}] using context classloader {}.", resource, classLoader);
                url = classLoader.getResource(resource);
                if (url != null) {
                    return url;
                }
            }

            classLoader = ResourceLoader.class.getClassLoader();
            if (classLoader != null) {
                LOGGER.debug("Trying to find [{}] using {} class loader.", resource, classLoader);
                url = classLoader.getResource(resource);
                if (url != null) {
                    return url;
                }
            }
        } catch (IllegalAccessException e) {
            LOGGER.warn("Caught exception while in ResourceLoader.getResource. This may be innocuous.", e);
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof InterruptedException || e.getTargetException() instanceof InterruptedIOException) {
                Thread.currentThread().interrupt();
            }

            LOGGER.warn("Caught exception while in ResourceLoader.getResource. This may be innocuous.", e);
        } catch (Throwable throwable) {
            LOGGER.warn("Caught exception while in ResourceLoader.getResource. This may be innocuous.", throwable);
        }

        LOGGER.debug("Trying to find [{}] using ClassLoader.getSystemResource().", resource);
        return ClassLoader.getSystemResource(resource);
    }
}
