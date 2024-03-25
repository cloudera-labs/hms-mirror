/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.connections;

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.Environment;
import com.cloudera.utils.hms.mirror.HiveServer2Config;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.util.DriverUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Slf4j
public class ConnectionPoolsDBCP2Impl implements ConnectionPools {

    private final Map<Environment, PoolingDataSource<PoolableConnection>> hs2DataSources = new TreeMap<>();
    private final Map<Environment, Driver> hs2Drivers = new TreeMap<>();
    private final Map<Environment, HiveServer2Config> hiveServerConfigs = new TreeMap<>();
    private final Map<Environment, DBStore> metastoreDirectConfigs = new TreeMap<>();
    private final Map<Environment, PoolingDataSource<PoolableConnection>> metastoreDirectDataSources = new TreeMap<>();
    @Getter
    private final ConfigService configService;

    public ConnectionPoolsDBCP2Impl(ConfigService configService) {
        this.configService = configService;
    }

    public void addHiveServer2(Environment environment, HiveServer2Config hiveServer2) {
        hiveServerConfigs.put(environment, hiveServer2);
    }

    public void addMetastoreDirect(Environment environment, DBStore dbStore) {
        metastoreDirectConfigs.put(environment, dbStore);
    }

    public void close() {
        try {
            if (hs2DataSources.get(Environment.LEFT) != null)
                hs2DataSources.get(Environment.LEFT).close();
        } catch (SQLException throwables) {
            //
        }
        try {
            if (hs2DataSources.get(Environment.RIGHT) != null)
                hs2DataSources.get(Environment.RIGHT).close();
        } catch (SQLException throwables) {
            //
        }
        try {
            if (metastoreDirectDataSources.get(Environment.LEFT) != null)
                metastoreDirectDataSources.get(Environment.LEFT).close();
        } catch (SQLException throwables) {
            //
        }
        try {
            if (metastoreDirectDataSources.get(Environment.RIGHT) != null)
                metastoreDirectDataSources.get(Environment.RIGHT).close();
        } catch (SQLException throwables) {
            //
        }
    }

    public synchronized Connection getHS2EnvironmentConnection(Environment environment) throws SQLException {
        Driver lclDriver = getHS2EnvironmentDriver(environment);
        Connection conn = null;
        if (lclDriver != null) {
            DriverManager.registerDriver(lclDriver);
            try {
                DataSource ds = getHS2EnvironmentDataSource(environment);
                if (ds != null)
                    conn = ds.getConnection();
            } catch (Throwable se) {
                se.printStackTrace();
                log.error(se.getMessage(), se);
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
        if (!getConfigService().getConfig().isLoadingTestData()) {
            initHS2Drivers();
            initHS2PooledDataSources();
            // Only init if we are going to use it. (`-epl`).
            if (getConfigService().loadPartitionMetadata()) {
                initMetastoreDataSources();
            }
        }

    }

    protected void initHS2Drivers() throws SQLException {
        Set<Environment> environments = new HashSet<>();
        environments.add(Environment.LEFT);
        environments.add(Environment.RIGHT);

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
                ConnectionFactory connectionFactory =
                        new DriverManagerConnectionFactory(hs2Config.getUri(), hs2Config.getConnectionProperties());

                PoolableConnectionFactory poolableConnectionFactory =
                        new PoolableConnectionFactory(connectionFactory, null);

                ObjectPool<PoolableConnection> connectionPool =
                        new GenericObjectPool<>(poolableConnectionFactory);

                poolableConnectionFactory.setPool(connectionPool);

                PoolingDataSource<PoolableConnection> poolingDatasource = new PoolingDataSource<>(connectionPool);
//            poolingDatasource.setLoginTimeout(10);

                hs2DataSources.put(environment, poolingDatasource);
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
                ConnectionFactory msconnectionFactory =
                        new DriverManagerConnectionFactory(metastoreDirectConfig.getUri(), metastoreDirectConfig.getConnectionProperties());

                PoolableConnectionFactory mspoolableConnectionFactory =
                        new PoolableConnectionFactory(msconnectionFactory, null);

                ObjectPool<PoolableConnection> msconnectionPool =
                        new GenericObjectPool<>(mspoolableConnectionFactory);

                mspoolableConnectionFactory.setPool(msconnectionPool);
                metastoreDirectDataSources.put(environment, new PoolingDataSource<>(msconnectionPool));
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
