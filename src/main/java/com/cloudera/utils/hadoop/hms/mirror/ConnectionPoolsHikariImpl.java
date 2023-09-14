/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hadoop.hms.mirror;

import com.cloudera.utils.hadoop.hms.Context;
import com.cloudera.utils.hadoop.hms.util.DriverUtils;
import com.cloudera.utils.hive.config.DBStore;
import com.google.common.collect.Sets;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

public class ConnectionPoolsHikariImpl implements ConnectionPools {
    private static final Logger LOG = LogManager.getLogger(ConnectionPools.class);
    private final Map<Environment, HikariDataSource> hs2DataSources = new TreeMap<>();
    private final Map<Environment, Driver> hs2Drivers = new TreeMap<>();
    private final Map<Environment, HiveServer2Config> hiveServerConfigs = new TreeMap<>();
    private final Map<Environment, DBStore> metastoreDirectConfigs = new TreeMap<>();
    private final Map<Environment, HikariDataSource> metastoreDirectDataSources = new TreeMap<>();

    public void addHiveServer2(Environment environment, HiveServer2Config hiveServer2) {
        hiveServerConfigs.put(environment, hiveServer2);
    }

    public void addMetastoreDirect(Environment environment, DBStore dbStore) {
        metastoreDirectConfigs.put(environment, dbStore);
    }

    public void close() {
        if (hs2DataSources.get(Environment.LEFT) != null)
            hs2DataSources.get(Environment.LEFT).close();

        if (hs2DataSources.get(Environment.RIGHT) != null)
            hs2DataSources.get(Environment.RIGHT).close();

        if (metastoreDirectDataSources.get(Environment.LEFT) != null)
            metastoreDirectDataSources.get(Environment.LEFT).close();

        if (metastoreDirectDataSources.get(Environment.RIGHT) != null)
            metastoreDirectDataSources.get(Environment.RIGHT).close();
    }

    public synchronized Connection getHS2EnvironmentConnection(Environment environment) throws SQLException {
        Driver lclDriver = getHS2EnvironmentDriver(environment);
        Connection conn = null;
        if (lclDriver != null) {
            DriverManager.registerDriver(lclDriver);
            try {
                DataSource ds = getHS2EnvironmentDataSource(environment);
                if (ds != null) {
                    conn = ds.getConnection();
                }
            } catch (Throwable se) {
                se.printStackTrace();
                LOG.error(se);
                throw new RuntimeException(se);
            } finally {
                DriverManager.deregisterDriver(lclDriver);
            }
        }
        return conn;
    }

    protected DataSource getHS2EnvironmentDataSource(Environment environment) {
        return hs2DataSources.get(environment);
    }

    protected synchronized Driver getHS2EnvironmentDriver(Environment environment) {
        return hs2Drivers.get(environment);
    }

    public synchronized Connection getMetastoreDirectEnvironmentConnection(Environment environment) throws SQLException {
        Connection conn = null;
        DataSource ds = getMetastoreDirectEnvironmentDataSource(environment);
        if (ds != null)
            conn = ds.getConnection();
        return conn;
    }

    protected DataSource getMetastoreDirectEnvironmentDataSource(Environment environment) {
        return metastoreDirectDataSources.get(environment);
    }

    public void init() throws SQLException {
        if (!Context.getInstance().getConfig().isLoadingTestData()) {
            initHS2Drivers();
            initHS2PooledDataSources();
            // Only init if we are going to use it. (`-epl`).
            if (Context.getInstance().loadPartitionMetadata()) {
                initMetastoreDataSources();
            }
        }
    }

    protected void initHS2Drivers() throws SQLException {
        Set<Environment> environments = Sets.newHashSet(Environment.LEFT, Environment.RIGHT);

        for (Environment environment : environments) {
            HiveServer2Config hs2Config = hiveServerConfigs.get(environment);
            if (hs2Config != null) {
                Driver driver = DriverUtils.getDriver(hs2Config.getDriverClassName(), hs2Config.getJarFile(), environment);
                // Need to deregister, cause it was registered in the getDriver.
                try {
                    DriverManager.deregisterDriver(driver);
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                    throw throwables;
                }
                hs2Drivers.put(environment, driver);
            }
        }
    }

    protected void initHS2PooledDataSources() {
        Set<Environment> environments = hiveServerConfigs.keySet();

        for (Environment environment : environments) {
            HiveServer2Config hs2Config = hiveServerConfigs.get(environment);
            if (!hs2Config.isDisconnected()) {

                Driver lclDriver = getHS2EnvironmentDriver(environment);
                if (lclDriver != null) {
                    try {
                        DriverManager.registerDriver(lclDriver);
                        try {
                            Properties props = new Properties();
                            if (hs2Config.getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
                                // Need with Apache Hive Driver, since it doesn't support
                                //      Connection.isValid() api (JDBC4) and prevents Hikari-CP from attempting to call it.
                                props.put("connectionTestQuery", "SELECT 1");
                            }
                            HikariConfig config = new HikariConfig(props);
                            config.setJdbcUrl(hs2Config.getUri());
                            config.setDataSourceProperties(hs2Config.getConnectionProperties());
                            HikariDataSource poolingDatasource = new HikariDataSource(config);

                            hs2DataSources.put(environment, poolingDatasource);
                        } catch (Throwable se) {
                            se.printStackTrace();
                            LOG.error(se);
                            throw new RuntimeException(se);
                        } finally {
                            DriverManager.deregisterDriver(lclDriver);
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                        LOG.error(e);
                        throw new RuntimeException(e);
                    }
                }

                Connection conn = null;
                try {
                    conn = getHS2EnvironmentConnection(environment);
                } catch (Throwable t) {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        throw new RuntimeException(t);
                    }
                }
            }
        }
    }

    protected void initMetastoreDataSources() {
        // Metastore Direct
        Set<Environment> environments = metastoreDirectConfigs.keySet();
        for (Environment environment : environments) {
            DBStore metastoreDirectConfig = metastoreDirectConfigs.get(environment);

            if (metastoreDirectConfig != null) {

                HikariConfig config = new HikariConfig();
                config.setJdbcUrl(metastoreDirectConfig.getUri());
                config.setDataSourceProperties(metastoreDirectConfig.getConnectionProperties());
                HikariDataSource poolingDatasource = new HikariDataSource(config);

                metastoreDirectDataSources.put(environment, poolingDatasource);
                // Test Connection.
                Connection conn = null;
                try {
                    conn = getMetastoreDirectEnvironmentConnection(environment);
                } catch (Throwable t) {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        throw new RuntimeException(t);
                    }
                }
            }
        }
    }

}
