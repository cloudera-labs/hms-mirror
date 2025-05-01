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
import com.cloudera.utils.hms.util.ConfigUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Slf4j
public class ConnectionPoolsHikariImpl extends ConnectionPoolsBase implements ConnectionPools {

    public ConnectionPoolsHikariImpl(ExecuteSession executeSession, PasswordService passwordService) {
        this.executeSession = executeSession;
        this.passwordService = passwordService;
    }

    protected void initHS2PooledDataSources() throws SessionException, EncryptionException {
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

                            // We need to review any property overrides for the environment to see
                            //   if they're trying to set the queue. EG tez.queue.name or mapred.job.queue.name
                            String queueOverride = ConfigUtils.getQueuePropertyOverride(environment, executeSession.getConfig());
                            if (queueOverride != null) {
                                props.put("connectionInitSql", queueOverride);
                            }

                            // Set the Concurrency
                            props.put("maximumPoolSize", executeSession.getConcurrency());
                            String hct = hs2Config.getConnectionProperties().getProperty(HIKARI_CONNECTION_TIMEOUT, HIKARI_CONNECTION_TIMEOUT_DEFAULT);
                            if (isBlank(hct)) {
                                hct = HIKARI_CONNECTION_TIMEOUT_DEFAULT;
                            }
                            props.put("connectionTimeout", Integer.parseInt(hct));

                            String vto = hs2Config.getConnectionProperties().getProperty(HIKARI_VALIDATION_TIMEOUT, HIKARI_VALIDATION_TIMEOUT_DEFAULT);
                            if (isBlank(vto)) {
                                vto = HIKARI_VALIDATION_TIMEOUT_DEFAULT;
                            }
                            props.put("validationTimeout", Integer.parseInt(vto));

                            String ift = hs2Config.getConnectionProperties().getProperty(HIKARI_INITIALIZATION_FAIL_TIMEOUT, HIKARI_INITIALIZATION_FAIL_TIMEOUT_DEFAULT);
                            if (isBlank(ift)) {
                                ift = HIKARI_INITIALIZATION_FAIL_TIMEOUT_DEFAULT;
                            }
                            props.put("initializationFailTimeout", Integer.parseInt(ift));

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
                            log.error("Issue closing HS2 connection for the {}", environment, e);
//                            throw new RuntimeException(e);
                        }
//                    } else {
//                        throw new RuntimeException(t);
                    }
                }
            }
        }
    }


}
