package com.streever.hadoop.hms.mirror;

import com.streever.hadoop.hms.util.DriverUtils;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ConnectionPools {

    private Map<Environment, PoolingDataSource<PoolableConnection>> dataSources =
            new TreeMap<Environment, PoolingDataSource<PoolableConnection>>();
    private Map<Environment, Driver> drivers = new TreeMap<Environment, Driver>();
    private Map<Environment, HiveServer2Config> hiveServerConfigs = new TreeMap<Environment, HiveServer2Config>();

    public void addHiveServer2(Environment environment, HiveServer2Config hiveServer2) {
        hiveServerConfigs.put(environment, hiveServer2);
    }

    public void init() {
        initDrivers();
        initPooledDataSources();
    }

    protected void initDrivers() {
        Set<Environment> environments = hiveServerConfigs.keySet();

        for (Environment environment : environments) {
            HiveServer2Config hs2Config = hiveServerConfigs.get(environment);
            Driver driver = DriverUtils.getDriver(hs2Config.getJarFile());
            // Need to deregister, cause it was registered in the getDriver.
            try {
                DriverManager.deregisterDriver(driver);
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            drivers.put(environment, driver);
        }
    }

    protected void initPooledDataSources() {
        Set<Environment> environments = hiveServerConfigs.keySet();

        for (Environment environment : environments) {
            HiveServer2Config hs2Config = hiveServerConfigs.get(environment);

            ConnectionFactory connectionFactory =
                    new DriverManagerConnectionFactory(hs2Config.getUri(), hs2Config.getConnectionProperties());

            PoolableConnectionFactory poolableConnectionFactory =
                    new PoolableConnectionFactory(connectionFactory, null);

            ObjectPool<PoolableConnection> connectionPool =
                    new GenericObjectPool<>(poolableConnectionFactory);

            poolableConnectionFactory.setPool(connectionPool);

            dataSources.put(environment, new PoolingDataSource<>(connectionPool));
        }
    }

    public synchronized Connection getEnvironmentConnection(Environment environment) throws SQLException {
        Driver lclDriver = getEnvironmentDriver(environment);
        DriverManager.registerDriver(lclDriver);
        Connection conn = null;
        try {
            conn = getEnvironmentDataSource(environment).getConnection();
        } catch (Throwable se) {
            se.printStackTrace();
            throw new RuntimeException(se);
        } finally {
            DriverManager.deregisterDriver(lclDriver);
        }
        return conn;
    }

    protected Driver getEnvironmentDriver(Environment environment) {
        return drivers.get(environment);
    }

    protected DataSource getEnvironmentDataSource(Environment environment) {
        return dataSources.get(environment);
    }

    public void close() {
        try {
            dataSources.get(Environment.LEFT).close();
        } catch (SQLException throwables) {
            //
        }
        try {
            dataSources.get(Environment.RIGHT).close();
        } catch (SQLException throwables) {
            //
        }
    }

}
