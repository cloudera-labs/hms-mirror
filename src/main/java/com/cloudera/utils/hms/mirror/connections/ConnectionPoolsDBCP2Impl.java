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
import com.cloudera.utils.hms.mirror.domain.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.PasswordService;
import com.cloudera.utils.hms.util.DriverUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;

import static java.util.Objects.nonNull;

@Slf4j
public class ConnectionPoolsDBCP2Impl extends ConnectionPoolsBase implements ConnectionPools {

    public ConnectionPoolsDBCP2Impl(ExecuteSession executeSession, PasswordService passwordService) {
        this.executeSession = executeSession;
        this.passwordService = passwordService;
    }

    protected void initHS2PooledDataSources() throws SessionException, EncryptionException {
        Set<Environment> environments = hiveServerConfigs.keySet();

        for (Environment environment : environments) {
            HiveServer2Config hs2Config = hiveServerConfigs.get(environment);
            if (!hs2Config.isDisconnected()) {
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

    @Override
    protected void initMetastoreDataSources() throws SessionException, EncryptionException {
        // Metastore Direct
        Set<Environment> environments = metastoreDirectConfigs.keySet();
        for (Environment environment : environments) {
            DBStore metastoreDirectConfig = metastoreDirectConfigs.get(environment);

            if (metastoreDirectConfig != null) {

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

                ConnectionFactory msconnectionFactory =
                        new DriverManagerConnectionFactory(metastoreDirectConfig.getUri(), connProperties);

                PoolableConnectionFactory mspoolableConnectionFactory =
                        new PoolableConnectionFactory(msconnectionFactory, null);

                ObjectPool<PoolableConnection> msconnectionPool =
                        new GenericObjectPool<>(mspoolableConnectionFactory);

                mspoolableConnectionFactory.setPool(msconnectionPool);

                // Attempt to get the Driver Version for the Metastore Direct Connection.
                try {
                    DataSource ds = getMetastoreDirectEnvironmentDataSource(environment);
                    Class driverClass = DriverManager.getDriver(ds.getConnection().getMetaData().getURL()).getClass();
                    String jarFile = DriverUtils.byGetProtectionDomain(driverClass);
                    log.info("{} - Metastore Direct JDBC JarFile: {}", environment, jarFile);
                } catch (SQLException | URISyntaxException e) {
                    log.error("Issue getting Metastore Direct JDBC JarFile details", e);
//                    throw new RuntimeException(e);
                }

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
