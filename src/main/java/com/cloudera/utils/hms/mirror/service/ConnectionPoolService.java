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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.connections.ConnectionPools;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsDBCP2Impl;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsHikariImpl;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsHybridImpl;
import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import static com.cloudera.utils.hms.mirror.MessageCode.ENVIRONMENT_CONNECTION_ISSUE;
import static com.cloudera.utils.hms.mirror.MessageCode.ENVIRONMENT_DISCONNECTED;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Component
@Getter
@Setter
@Slf4j
public class ConnectionPoolService {

    private boolean connected = false;

//    private HmsMirrorConfig hmsMirrorConfig;
    private ExecuteSession executeSession;
    private ConnectionPools connectionPools = null;
    private EnvironmentService environmentService;

    private PasswordService passwordService;


    @Autowired
    public void setEnvironmentService(EnvironmentService environmentService) {
        this.environmentService = environmentService;
    }

    @Autowired
    public void setPasswordService(PasswordService passwordService) {
        this.passwordService = passwordService;
    }

    public Boolean checkConnections() {
        boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = executeSession.getConfig();

        if (isNull(config)) {
            log.error("Configuration not set.  Connection can't be established");
            return Boolean.FALSE;
//            throw new RuntimeException("Configuration not set.  Connections can't be established.");
        }

        Set<Environment> envs = new HashSet<>();
        if (!(config.getDataStrategy() == DataStrategyEnum.DUMP ||
                config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION ||
                config.getDataStrategy() == DataStrategyEnum.ICEBERG_CONVERSION)) {
            envs.add(Environment.LEFT);
            envs.add(Environment.RIGHT);
        } else {
            envs.add(Environment.LEFT);
        }

        for (Environment env : envs) {
            Cluster cluster = config.getCluster(env);
            if (nonNull(cluster)
                    && nonNull(cluster.getHiveServer2())
                    && cluster.getHiveServer2().isValidUri()
                    && !cluster.getHiveServer2().isDisconnected()) {
                Connection conn = null;
                try {
                    conn = getConnectionPools().getHS2EnvironmentConnection(env);
                    log.debug("{}:: Checking Hive Connection", env);
                    if (nonNull(conn)) {
                        log.debug("{}:: Hive Connection Successful", env);
                        rtn = Boolean.TRUE;
                    } else {
                        log.error("{}: Hive Connection check failed.  Connection is null.", env);
                        rtn = Boolean.FALSE;
                    }
                } catch (SQLException se) {
                    rtn = Boolean.FALSE;
                    log.error("{}: Hive Connection check failed.", env, se);
                } finally {
                    if (nonNull(conn)) {
                        try {
                            log.info("{}: Closing Connection", env);
                            conn.close();
                        } catch (Throwable throwables) {
                            log.error("{}: Error closing connections.", env, throwables);
                        }
                    }
                }
            }
        }
        return rtn;
    }

//    @Override
//    public void addHiveServer2(Environment environment, HiveServer2Config hiveServer2) {
//        getConnectionPools().addHiveServer2(environment, hiveServer2);
//    }
//
//    @Override
//    public void addMetastoreDirect(Environment environment, DBStore dbStore) {
//        getConnectionPools().addMetastoreDirect(environment, dbStore);
//    }

    public void close() {
        if (nonNull(connectionPools)) {
            // Set State of Connection.
            connected = false;
            getConnectionPools().close();
            // Set to null to allow for reset.
            connectionPools = null;
        }
    }

    public ConnectionPools getConnectionPools() {
        if (isNull(connectionPools)) {
            try {
                connectionPools = getConnectionPoolsImpl();
            } catch (SQLException e) {
                log.error("Error creating connections pools", e);
//                throw new RuntimeException(e);
            }
        }
        return connectionPools;
    }

    private ConnectionPools getConnectionPoolsImpl() throws SQLException {
        ConnectionPools rtn = null;
        HmsMirrorConfig config = executeSession.getConfig();

        if (isNull(config)) {
            log.error("Configuration not set.  Connections can't be established.");
            return null;
//            throw new RuntimeException("Configuration not set.  Connections can't be established.");
        }
        ConnectionPoolType cpt = config.getConnectionPoolLib();
        if (isNull(cpt)) {
            // Need to calculate the connectio pool type:
            // When both clusters are defined:
                // Use DBCP2 when both clusters are non-legacy
                // Use HYBRID when one cluster is legacy and the other is not
                // Use HIKARICP when both clusters are non-legacy.
            // When only the left cluster is defined:
                // Use DBCP2 when the left cluster is legacy
                // Use HIKARICP when the left cluster is non-legacy
            if (isNull(config.getCluster(Environment.RIGHT))) {
                if (config.getCluster(Environment.LEFT).isLegacyHive()) {
                    cpt = ConnectionPoolType.DBCP2;
                } else {
                    cpt = ConnectionPoolType.HIKARICP;
                }
            } else {
                if (config.getCluster(Environment.LEFT).isLegacyHive()
                        && config.getCluster(Environment.RIGHT).isLegacyHive()) {
                    cpt = ConnectionPoolType.DBCP2;
                } else if (config.getCluster(Environment.LEFT).isLegacyHive()
                        || config.getCluster(Environment.RIGHT).isLegacyHive()) {
                    cpt = ConnectionPoolType.HYBRID;
                } else {
                    cpt = ConnectionPoolType.HIKARICP;
                }
            }
        } else {
            log.info("Connection Pool Type explicitly set to: {}", cpt);
        }
        switch (cpt) {
            case DBCP2:
                log.info("Using DBCP2 Connection Pooling Libraries");
                rtn = new ConnectionPoolsDBCP2Impl(executeSession, passwordService);
                break;
            case HIKARICP:
                log.info("Using HIKARICP Connection Pooling Libraries");
                rtn = new ConnectionPoolsHikariImpl(executeSession, passwordService);
                break;
            case HYBRID:
                log.info("Using HYBRID Connection Pooling Libraries");
                rtn = new ConnectionPoolsHybridImpl(executeSession, passwordService);
                break;
        }
        // Initialize the connections pools
//        rtn.init();
        return rtn;
    }

//    @Override
    public Connection getHS2EnvironmentConnection(Environment environment) throws SQLException {
        Connection conn = getConnectionPools().getHS2EnvironmentConnection(environment);
        return conn;
    }

//    @Override
    public Connection getMetastoreDirectEnvironmentConnection(Environment environment) throws SQLException {
        Connection conn = getConnectionPools().getMetastoreDirectEnvironmentConnection(environment);
        return conn;
    }

    public void reset() throws SQLException, EncryptionException, SessionException {
        close();
        init();
    }

//    @Override
    public void init() throws SQLException, SessionException, EncryptionException {
//        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
//        ExecuteSession executeSession = executeSessionService.getActiveSession();
        HmsMirrorConfig config = executeSession.getConfig();
        RunStatus runStatus = executeSession.getRunStatus();

        if (config.getDataStrategy() == DataStrategyEnum.DUMP) {
            config.setExecute(Boolean.FALSE); // No Actions.
            config.setSync(Boolean.FALSE);
        }

        // Make adjustments to the config clusters based on settings.
        // Buildout the right connections pool details.
        Set<Environment> hs2Envs = new HashSet<Environment>();
        switch (config.getDataStrategy()) {
            case DUMP:
                // Don't load the datasource for the right with DUMP strategy.
                if (config.getDumpSource() == Environment.RIGHT) {
                    // switch LEFT and RIGHT
                    config.getClusters().remove(Environment.LEFT);
                    config.getClusters().put(Environment.LEFT, config.getCluster(Environment.RIGHT));
                    config.getCluster(Environment.LEFT).setEnvironment(Environment.LEFT);
                    config.getClusters().remove(Environment.RIGHT);
                }
            case STORAGE_MIGRATION:
                // Get Pool
                getConnectionPools().addHiveServer2(Environment.LEFT, config.getCluster(Environment.LEFT).getHiveServer2());
                hs2Envs.add(Environment.LEFT);
                break;
            case SQL:
            case COMMON:
            case LINKED:
            case SCHEMA_ONLY:
            case EXPORT_IMPORT:
            case HYBRID:
                // When doing inplace downgrade of ACID tables, we're only dealing with the LEFT cluster.
                if (!config.getMigrateACID().isInplace() && null != config.getCluster(Environment.RIGHT).getHiveServer2()) {
                    getConnectionPools().addHiveServer2(Environment.RIGHT, config.getCluster(Environment.RIGHT).getHiveServer2());
                    hs2Envs.add(Environment.RIGHT);
                }
            default:
                getConnectionPools().addHiveServer2(Environment.LEFT, config.getCluster(Environment.LEFT).getHiveServer2());
                hs2Envs.add(Environment.LEFT);
                break;
        }
//        if (getConfig().loadMetadataDetails()) {
            if (nonNull(config.getCluster(Environment.LEFT)) &&
                    nonNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
                getConnectionPools().addMetastoreDirect(Environment.LEFT, config.getCluster(Environment.LEFT).getMetastoreDirect());
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) &&
                    nonNull(config.getCluster(Environment.RIGHT).getMetastoreDirect())) {
                getConnectionPools().addMetastoreDirect(Environment.RIGHT, config.getCluster(Environment.RIGHT).getMetastoreDirect());
            }
//        }

        if (config.isConnectionKerberized()) {
            log.debug("Detected a Kerberized JDBC Connection.  Attempting to setup/initialize GSS.");
            environmentService.setupGSS();
        }

        try {
            // TODO: Should we try to close first to clean up any existing connections?
            getConnectionPools().init();
            for (Environment target : hs2Envs) {
                Connection conn = null;
                Statement stmt = null;
                try {
                    conn = getConnectionPools().getHS2EnvironmentConnection(target);
                    if (isNull(conn)) {
                        if (target == Environment.RIGHT && config.getCluster(target).getHiveServer2().isDisconnected()) {
                            // Skip error.  Set Warning that we're disconnected.
                            runStatus.addWarning(ENVIRONMENT_DISCONNECTED, target);
                        } else if (!config.isLoadingTestData()) {
                            runStatus.addError(ENVIRONMENT_CONNECTION_ISSUE, target);
                        }
                    } else {
                        // Exercise the connections.
                        stmt = conn.createStatement();
                        stmt.execute("SELECT 1");
                    }
                } catch (SQLException se) {
                    if (target == Environment.RIGHT && config.getCluster(target).getHiveServer2().isDisconnected()) {
                        // Set warning that RIGHT is disconnected.
                        runStatus.addWarning(ENVIRONMENT_DISCONNECTED, target);
                    } else {
                        log.error(se.getMessage(), se);
                        runStatus.addError(ENVIRONMENT_CONNECTION_ISSUE, target);
                    }
                } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                    runStatus.addError(ENVIRONMENT_CONNECTION_ISSUE, target);
                } finally {
                    if (nonNull(stmt)) {
                        stmt.close();
                    }
                    if (nonNull(conn)) {
                        conn.close();
                    }
                }
            }
        } catch (SQLException | URISyntaxException cnfe) {
            getConnectionPools().close();
            log.error("Issue initializing connections.  Check driver locations", cnfe);
            throw new RuntimeException(cnfe);
        }

//            hmsMirrorConfig.getCluster(Environment.LEFT).setPools(connectionPoolService.getConnectionPools());
//        switch (getConfig().getDataStrategy()) {
//            case DUMP:
//                // Don't load the datasource for the right with DUMP strategy.
//                break;
//            default:
//                // Don't set the Pools when Disconnected.
//                if (nonNull(getConfig().getCluster(Environment.RIGHT))
//                        && nonNull(getConfig().getCluster(Environment.RIGHT).getHiveServer2())
//                        && !getConfig().getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()) {
////                        getConfig().getCluster(Environment.RIGHT).setPools(connectionPoolService.getConnectionPools());
//                }
//        }

        log.debug("Checking Hive Connections");
        if (!config.isLoadingTestData() && !checkConnections()) {
            log.error("Check Hive Connections Failed.");
            if (config.isConnectionKerberized()) {
                log.error("Check Kerberos configuration if GSS issues are encountered.  See the running.md docs for details.");
            }
//            throw new RuntimeException("Check Hive Connections Failed.  Check Logs.");
        }
        // Set state of connections.
        connected = true;
        executeSession.setConnected(Boolean.TRUE);
    }

}
