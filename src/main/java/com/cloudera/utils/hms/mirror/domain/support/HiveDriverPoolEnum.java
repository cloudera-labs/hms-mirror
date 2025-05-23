package com.cloudera.utils.hms.mirror.domain.support;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Slf4j
public enum HiveDriverPoolEnum {
    HIKARI("com.zaxxer.hikari.HikariDataSource", "hikari", Arrays.asList("allowPoolSuspension","autoCommit",
            "catalog","connectionInitSql","connectionTestQuery","connectionTimeout","dataSource","driverClassName",
            "exceptionOverride","exceptionOverrideClassName","healthCheckRegistry","idleTimeout",
            "initializationFailTimeout","isolateInternalQueries","keepaliveTime","leakDetectionThreshold",
            "maximumPoolSize","maxLifetime","metricRegistry","minimumIdle","password","poolName","readOnly",
            "registerMbeans","scheduledExecutor","schema","threadFactory","transactionIsolation","username",
            "validationTimeout")),
    DBCP2("org.apache.commons.dbcp2.BasicDataSource", "dbcp2", Arrays.asList("abandonedUsageTracking",
            "accessToUnderlyingConnectionAllowed","cacheState","connectionInitSqls","connectionProperties",
            "defaultAutoCommit","defaultCatalog","defaultQueryTimeout","defaultReadOnly","defaultTransactionIsolation",
            "disconnectionIgnoreSqlCodes","disconnectionSqlCodes","driverClassName","enableAutoCommitOnReturn",
            "fastFailValidation","initialSize","jmxName","lifo","logAbandoned","logExpiredConnections",
            "maxConnLifetimeMillis","maxIdle","maxOpenPreparedStatements","maxTotal","maxWaitMillis",
            "minEvictableIdleTimeMillis","minIdle","numTestsPerEvictionRun","password","poolPreparedStatements",
            "registerConnectionMBean","removeAbandonedOnBorrow","removeAbandonedOnMaintenance ","removeAbandonedTimeout",
            "rollbackOnReturn","softMinEvictableIdleTimeMillis","testOnBorrow","testOnCreate","testOnReturn",
            "testWhileIdle","timeBetweenEvictionRunsMillis","url","username","validationQuery","validationQueryTimeout"));

    private final String driverClassName;
    private final String prefix;
    // A list of supported driver parameters
    private final List<String> driverParameters;
    
    HiveDriverPoolEnum(String driverClassName, String prefix, List<String> driverParameters) {
        this.driverClassName = driverClassName;
        this.prefix = prefix;
        this.driverParameters = driverParameters;
    }
    public static String[] getDriverClassNames() {
        return Arrays.stream(HiveDriverPoolEnum.values())
                .map(HiveDriverPoolEnum::getDriverClassName)
                .toArray(String[]::new);
    }

    // Get List<String> of Driver Prefixes
    public static List<String> getDriverPrefixes() {
        return Arrays.stream(HiveDriverPoolEnum.values())
                .map(HiveDriverPoolEnum::getPrefix)
                .toList();
    }

    public static HiveDriverPoolEnum getDriverPool(String driverClassName) {
        return Arrays.stream(HiveDriverPoolEnum.values())
                .filter(driver -> driver.getDriverClassName().equals(driverClassName))
                .findFirst()
                .orElse(null);
    }
    public String getDriverClassName() {
        return driverClassName;
    }

    public String getPrefix() {
        return prefix;
    }

    public List<String> getDriverParameters() {
        return driverParameters;
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
        for (String key : properties.stringPropertyNames()) {
            if (driverParameters.contains(key)) {
                reconciledProperties.put(key, properties.getProperty(key));
            } else {
                // TODO: Add and Warn OR omit and LOG.
                log.warn("Property " + key + " is not supported by driver " + driverClassName);
            }
        }
        return reconciledProperties;
    }

}
