/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms.mirror;

import com.google.common.collect.Sets;
import com.cloudera.utils.hadoop.hms.util.DriverUtils;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ConnectionPools {
    private static final Logger LOG = LogManager.getLogger(ConnectionPools.class);

    private final Map<Environment, PoolingDataSource<PoolableConnection>> dataSources =
            new TreeMap<Environment, PoolingDataSource<PoolableConnection>>();
    private final Map<Environment, Driver> drivers = new TreeMap<Environment, Driver>();
    private final Map<Environment, HiveServer2Config> hiveServerConfigs = new TreeMap<Environment, HiveServer2Config>();

    public void addHiveServer2(Environment environment, HiveServer2Config hiveServer2) {
        hiveServerConfigs.put(environment, hiveServer2);
    }

    public void init() {
        initDrivers();
        initPooledDataSources();
    }

    protected void initDrivers() {
        Set<Environment> environments = Sets.newHashSet(Environment.LEFT, Environment.RIGHT);

        for (Environment environment : environments) {
            HiveServer2Config hs2Config = hiveServerConfigs.get(environment);
            if (hs2Config != null) {
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

            PoolingDataSource poolingDatasource = new PoolingDataSource<>(connectionPool);
//            poolingDatasource.setLoginTimeout(10);

            dataSources.put(environment, poolingDatasource);
        }
    }

    public synchronized Connection getEnvironmentConnection(Environment environment) throws SQLException {
        Driver lclDriver = getEnvironmentDriver(environment);
        Connection conn = null;
        if (lclDriver != null) {
            DriverManager.registerDriver(lclDriver);
            try {
                conn = getEnvironmentDataSource(environment).getConnection();
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
