package spark.streaming;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.application.AbstractApplication;
import spark.streaming.util.Configuration;

/**
 *
 * @author mayconbordin
 */
public class AppDriver {
    private static final Logger LOG = LoggerFactory.getLogger(AppDriver.class);
    private final Map<String, AppDescriptor> applications;

    public AppDriver() {
        applications = new HashMap<>();
    }
    
    public void addApp(String name, Class<? extends AbstractApplication> cls) {
        applications.put(name, new AppDescriptor(cls));
    }
    
    public AppDescriptor getApp(String name) {
        return applications.get(name);
    }
    
    public static class AppDescriptor {
        private final Class<? extends AbstractApplication> cls;

        public AppDescriptor(Class<? extends AbstractApplication> cls) {
            this.cls = cls;
        }
        
        public DataStreamWriter<Row> getContext(String applicationName, Configuration config) {
            try {
                Constructor c = cls.getConstructor(String.class, Configuration.class);
                LOG.info("Loaded application {}", cls.getCanonicalName());

                AbstractApplication application = (AbstractApplication) c.newInstance(applicationName, config);
                application.initialize();
                return application.buildApplication();
            } catch (ReflectiveOperationException ex) {
                LOG.error("Unable to load application class", ex);
                return null;
            } catch (StreamingQueryException e) {
                LOG.error("Unable to query application");
                return null;
            }
        }
        public JavaStreamingContext getContextStreaming(String applicationName, Configuration config) {
            try {
                Constructor c = cls.getConstructor(String.class, Configuration.class);
                LOG.info("Loaded application {}", cls.getCanonicalName());

                AbstractApplication application = (AbstractApplication) c.newInstance(applicationName.split("-")[0], config);
                application.initialize();
                return application.buildApplicationStreaming();
            } catch (ReflectiveOperationException ex) {
                LOG.error("Unable to load application class", ex);
                return null;
            }
        }
    }
}
