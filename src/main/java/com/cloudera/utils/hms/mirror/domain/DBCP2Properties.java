package com.cloudera.utils.hms.mirror.domain;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Component
@ConfigurationProperties(prefix = "dbcp2")
@Getter
@Setter
public class DBCP2Properties {

    private String abandonedUsageTracking;
    private String accessToUnderlyingConnectionAllowed;
    private String cacheState;
    private String connectionInitSqls;
    private String connectionProperties;
    private String defaultAutoCommit;
    private String defaultCatalog;
    private String defaultQueryTimeout;
    private String defaultReadOnly;
    private String defaultTransactionIsolation;
    private String disconnectionIgnoreSqlCodes;
    private String disconnectionSqlCodes;
    private String driverClassName;
    private String enableAutoCommitOnReturn;
    private String fastFailValidation;
    private String initialSize;
    private String jmxName;
    private String lifo;
    private String logAbandoned;
    private String logExpiredConnections;
    private String maxConnLifetimeMillis;
    private String maxIdle;
    private String maxOpenPreparedStatements;
    private String maxTotal;
    private String maxWaitMillis;
    private String minEvictableIdleTimeMillis;
    private String minIdle;
    private String numTestsPerEvictionRun;
    private String password;
    private String poolPreparedStatements;
    private String registerConnectionMBean;
    private String removeAbandonedOnBorrow;
    private String removeAbandonedOnMaintenance;
    private String removeAbandonedTimeout;
    private String rollbackOnReturn;
    private String softMinEvictableIdleTimeMillis;
    private String testOnBorrow;
    private String testOnCreate;
    private String testOnReturn;
    private String testWhileIdle;
    private String timeBetweenEvictionRunsMillis;
    private String url;
    private String username;
    private String validationQuery;
    private String validationQueryTimeout;

    private String threads;

    @Value("${hms-mirror.concurrency.max-threads}")
    public void setThreads(String threads) {
        this.threads = threads;
        this.minIdle = Integer.valueOf(threads) / 2 + "";
        this.initialSize = Integer.valueOf(threads) / 2 + "";
        this.maxTotal = threads;
    }

    /**
     * Create a method that returns a properties object. If a property is not null, add it to a properties object with
     * the name of the property as the key and the value as the value. Use reflection to review the properties in this class.
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
