/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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
import com.cloudera.utils.hms.mirror.domain.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.PasswordService;
import com.cloudera.utils.hms.util.DriverUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.PoolingDataSource;

import javax.sql.DataSource;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Getter
@Setter
@Slf4j
public abstract class ConnectionPoolsBase implements ConnectionPools {

    private ConnectionState connectionState = ConnectionState.DISCONNECTED;
    protected ExecuteSession executeSession;
    protected PasswordService passwordService;

    protected final Map<Environment, DataSource> hs2DataSources = new TreeMap<>();
    protected final Map<Environment, Driver> hs2Drivers = new TreeMap<>();
    protected final Map<Environment, HiveServer2Config> hiveServerConfigs = new TreeMap<>();
    protected final Map<Environment, DBStore> metastoreDirectConfigs = new TreeMap<>();
    protected final Map<Environment, DataSource> metastoreDirectDataSources = new TreeMap<>();

    public void close() {
        try {
            if (hs2DataSources.get(Environment.LEFT) != null) {
                if (hs2DataSources.get(Environment.LEFT) instanceof PoolingDataSource) {
                    ((PoolingDataSource<?>) hs2DataSources.get(Environment.LEFT)).close();
                } else if (hs2DataSources.get(Environment.LEFT) instanceof HikariDataSource) {
                    ((HikariDataSource) hs2DataSources.get(Environment.LEFT)).close();
                }
            }
        } catch (SQLException throwables) {
            //
        }
        try {
            if (hs2DataSources.get(Environment.RIGHT) != null) {
                if (hs2DataSources.get(Environment.RIGHT) instanceof PoolingDataSource) {
                    ((PoolingDataSource<?>) hs2DataSources.get(Environment.RIGHT)).close();
                } else if (hs2DataSources.get(Environment.RIGHT) instanceof HikariDataSource) {
                    ((HikariDataSource) hs2DataSources.get(Environment.RIGHT)).close();
                }
            }
        } catch (SQLException throwables) {
            //
        }
        if (metastoreDirectDataSources.get(Environment.LEFT) != null)
            if (metastoreDirectDataSources.get(Environment.LEFT) instanceof PoolingDataSource) {
                try {
                    ((PoolingDataSource<?>)metastoreDirectDataSources.get(Environment.LEFT)).close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            else if (metastoreDirectDataSources.get(Environment.LEFT) instanceof HikariDataSource)
                ((HikariDataSource)metastoreDirectDataSources.get(Environment.LEFT)).close();

        if (metastoreDirectDataSources.get(Environment.RIGHT) != null)
            if (metastoreDirectDataSources.get(Environment.RIGHT) instanceof PoolingDataSource) {
                try {
                    ((PoolingDataSource<?>)metastoreDirectDataSources.get(Environment.RIGHT)).close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            else if (metastoreDirectDataSources.get(Environment.RIGHT) instanceof HikariDataSource)
                ((HikariDataSource)metastoreDirectDataSources.get(Environment.RIGHT)).close();
    }


    public void addHiveServer2(Environment environment, HiveServer2Config hiveServer2) {
        hiveServerConfigs.put(environment, hiveServer2);
    }

    public void addMetastoreDirect(Environment environment, DBStore dbStore) {
        metastoreDirectConfigs.put(environment, dbStore);
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

    protected void initHS2Drivers() throws SQLException {
        Set<Environment> environments = new HashSet<>();
        environments.add(Environment.LEFT);
        environments.add(Environment.RIGHT);

        for (Environment environment : environments) {
            HiveServer2Config hs2Config = hiveServerConfigs.get(environment);
            if (hs2Config != null) {
                Driver driver = DriverUtils.getDriver(hs2Config, environment);
                // Need to deregister, because it was registered in the getDriver.
                try {
                    DriverManager.deregisterDriver(driver);
                } catch (SQLException throwables) {
                    log.error(throwables.getMessage(), throwables);
                    throw throwables;
                }
                hs2Drivers.put(environment, driver);
            }
        }
    }


    protected abstract void initHS2PooledDataSources() throws SessionException, EncryptionException;

    protected void initMetastoreDataSources() throws SessionException, EncryptionException, SQLException, URISyntaxException {
        // Metastore Direct
        log.debug("Initializing Metastore Direct DataSources");
        Set<Environment> environments = metastoreDirectConfigs.keySet();
        for (Environment environment : environments) {
            DBStore metastoreDirectConfig = metastoreDirectConfigs.get(environment);

            if (nonNull(metastoreDirectConfig) && !isBlank(metastoreDirectConfig.getUri())) {

                // Make a copy.
                Properties connProperties = new Properties();
                connProperties.putAll(metastoreDirectConfig.getConnectionProperties());
                // If the ExecuteSession has the 'passwordKey' set, resolve Encrypted PasswordApp first.
                if (executeSession.getConfig().isEncryptedPasswords()) {
                    if (nonNull(executeSession.getConfig().getPasswordKey()) && !executeSession.getConfig().getPasswordKey().isEmpty()) {
                        String encryptedPassword = connProperties.getProperty("password");
                        String decryptedPassword = passwordService.decryptPassword(executeSession.getConfig().getPasswordKey(), encryptedPassword);
                        connProperties.setProperty("password", decryptedPassword);
                    } else {
                        throw new SessionException("Passwords encrypted, but no password key present.");
                    }
                }

                HikariConfig config = new HikariConfig();
                config.setJdbcUrl(metastoreDirectConfig.getUri());
                config.setDataSourceProperties(connProperties);
                // Attempt to get the Driver Version for the Metastore Direct Connection.
                try {
                    HikariDataSource poolingDatasource = new HikariDataSource(config);
                    metastoreDirectDataSources.put(environment, poolingDatasource);

                    DataSource ds = getMetastoreDirectEnvironmentDataSource(environment);
                    Class driverClass = DriverManager.getDriver(ds.getConnection().getMetaData().getURL()).getClass();
                    String jarFile = DriverUtils.byGetProtectionDomain(driverClass);
                    metastoreDirectConfig.setResource(jarFile);
                    log.info("{} - Metastore Direct JDBC JarFile: {}", environment, jarFile);
                    String version = ds.getConnection().getMetaData().getDriverVersion();
                    metastoreDirectConfig.setVersion(version);
                    log.info("{} - Metastore Direct JDBC Driver Version: {}", environment, version);
                } catch (SQLException | URISyntaxException e) {
                    log.error("Issue getting Metastore Direct JDBC JarFile details", e);
                    throw e;
                }

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

    public synchronized void init() throws SQLException, SessionException, EncryptionException, URISyntaxException {
        if (!executeSession.getConfig().isLoadingTestData()) {
            try {
                switch (connectionState) {
                    case DISCONNECTED:
                    case ERROR:
                        connectionState = ConnectionState.INITIALIZING;
                        // Attempt to re-init.
                        initHS2Drivers();
                        initHS2PooledDataSources();
                        initMetastoreDataSources();
                        break;
                    case INITIALIZING:
                    case INITIALIZED:
                    case CONNECTED:
                        // Do Nothing
                        break;
                }
                connectionState = ConnectionState.CONNECTED;
            } catch (SQLException | SessionException | EncryptionException | URISyntaxException e) {
                connectionState = ConnectionState.ERROR;
                throw e;
            }
        }
    }


}
