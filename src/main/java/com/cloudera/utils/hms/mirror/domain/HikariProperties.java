package com.cloudera.utils.hms.mirror.domain;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Component
@ConfigurationProperties(prefix = "hikari")
@Getter
@Setter
public class HikariProperties {

    private String allowPoolSuspension;
    private String autoCommit;
    private String catalog;
    private String connectionInitSql;
    private String connectionTestQuery;
    private String connectionTimeout;
    private String dataSource;
    private String driverClassName;
    private String exceptionOverride;
    private String exceptionOverrideClassName;
    private String healthCheckRegistry;
    private String idleTimeout;
    private String initializationFailTimeout;
    private String isolateInternalQueries;
    private String keepaliveTime;
    private String leakDetectionThreshold;
    private String maximumPoolSize;
    private String maxLifetime;
    private String metricRegistry;
    private String minimumIdle;
    private String password;
    private String poolName;
    private String readOnly;
    private String registerMbeans;
    private String scheduledExecutor;
    private String schema;
    private String threadFactory;
    private String transactionIsolation;
    private String username;
    private String validationTimeout;

    private String threads;

    @Value("${hms-mirror.concurrency.max-threads}")
    public void setThreads(String threads) {
        this.threads = threads;
        this.minimumIdle = Integer.valueOf(threads) / 2 + "";
        this.maximumPoolSize = threads;
    }

    /*
     Create a method that returns a properties object. If a property is not null, add it to a properties object with
     the name of the property as the key and the value as the value. Use reflection to review the properties in this class.
     */
    public Properties toProperties() {
        Properties properties = new Properties();
        for (java.lang.reflect.Field field : this.getClass().getDeclaredFields()) {
            // Skip the threads field
            if (field.getName().equals("threads")) {
                continue; // Skip the threads field
            }
            field.setAccessible(true);
            try {
                Object value = field.get(this);
                if (value != null) {
                    properties.put(field.getName(), value.toString());
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return properties;
    }

    /**
     * Remove any properties that aren't allowed by the driver.  Not allowed is defined as not being one of the
     * properties in this class.
     *
     * @param properties
     * @return
     */
    public Properties reconcileForDriver(Properties properties) {
        Properties reconciledProperties = new Properties();
        List<String> allowedProperties = new ArrayList<>();
        for (java.lang.reflect.Field field : this.getClass().getDeclaredFields()) {
            if (field.getName().equals("threads")) {
                continue; // Skip the threads field
            }
            allowedProperties.add(field.getName());
        }
        for (String key : properties.stringPropertyNames()) {
            if (allowedProperties.contains(key)) {
                reconciledProperties.put(key, properties.getProperty(key));
            }
        }
        return reconciledProperties;
    }

}
