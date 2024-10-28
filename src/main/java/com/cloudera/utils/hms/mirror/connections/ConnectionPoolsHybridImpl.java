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

import com.cloudera.utils.hms.mirror.domain.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.PasswordService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;

import static java.util.Objects.nonNull;

@Slf4j
public class ConnectionPoolsHybridImpl extends ConnectionPoolsBase implements ConnectionPools {

    public ConnectionPoolsHybridImpl(ExecuteSession executeSession, PasswordService passwordService) {
        this.executeSession = executeSession;
        this.passwordService = passwordService;
    }

    protected void initHS2PooledDataSources() throws SessionException, EncryptionException {
        Set<Environment> environments = hiveServerConfigs.keySet();

        for (Environment environment : environments) {
            HiveServer2Config hs2Config = hiveServerConfigs.get(environment);
            if (!hs2Config.isDisconnected()) {
                // Check for legacy.  If Legacy, use dbcp2 else hikaricp.
                if (executeSession.getConfig().getCluster(environment).isLegacyHive()) {
                    // Make a copy.
                    Properties connProperties = new Properties();
                    connProperties.putAll(hs2Config.getConnectionProperties());
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

                    ConnectionFactory connectionFactory =
                            new DriverManagerConnectionFactory(hs2Config.getUri(), connProperties);

                    PoolableConnectionFactory poolableConnectionFactory =
                            new PoolableConnectionFactory(connectionFactory, null);

                    ObjectPool<PoolableConnection> connectionPool =
                            new GenericObjectPool<>(poolableConnectionFactory);

                    poolableConnectionFactory.setPool(connectionPool);

                    PoolingDataSource<PoolableConnection> poolingDatasource = new PoolingDataSource<>(connectionPool);

                    hs2DataSources.put(environment, poolingDatasource);
                    Connection conn = null;
                    try {
                        conn = getHS2EnvironmentConnection(environment);
                    } catch (Throwable t) {
                        if (conn != null) {
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                log.error("Issue closing HS2 connection for the {}", environment, e);
                                throw new RuntimeException(e);
                            }
                        } else {
                            log.error("Connection null");
//                            throw new RuntimeException(t);
                        }
                    }
                } else {
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

                                // Make a copy.
                                Properties connProperties = new Properties();
                                connProperties.putAll(hs2Config.getConnectionProperties());
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

                                HikariConfig config = new HikariConfig(props);
                                config.setJdbcUrl(hs2Config.getUri());
                                config.setDataSourceProperties(connProperties);
                                HikariDataSource poolingDatasource = new HikariDataSource(config);

                                hs2DataSources.put(environment, poolingDatasource);
                            } catch (Throwable se) {
                                log.error(se.getMessage(), se);
                                throw new RuntimeException(se);
                            } finally {
                                DriverManager.deregisterDriver(lclDriver);
                            }
                        } catch (SQLException e) {
                            log.error(e.getMessage(), e);
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
                            log.error("Connection null");
//                            throw new RuntimeException(t);
                        }
                    }
                }
            }
        }
    }

}
