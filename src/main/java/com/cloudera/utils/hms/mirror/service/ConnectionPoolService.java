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

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.connections.ConnectionPools;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsDBCP2Impl;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsHikariImpl;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsHybridImpl;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.util.ConfigUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.cloudera.utils.hms.mirror.MessageCode.ENVIRONMENT_CONNECTION_ISSUE;
import static com.cloudera.utils.hms.mirror.MessageCode.ENVIRONMENT_DISCONNECTED;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

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
    private CliEnvironment cliEnvironment;
    private ConfigService configService;

    private PasswordService passwordService;

    @Autowired
    public void setEnvironmentService(EnvironmentService environmentService) {
        this.environmentService = environmentService;
    }

    @Autowired
    public void setPasswordService(PasswordService passwordService) {
        this.passwordService = passwordService;
    }

    @Autowired
    public void setCliEnvironment(CliEnvironment cliEnvironment) {
        this.cliEnvironment = cliEnvironment;
    }

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    public ExecuteSession getExecuteSession() throws SessionException {
        if (isNull(executeSession)) {
            throw new SessionException("Session hasn't been set in ConnectionPoolService");// = executeSessionService.getSession();
        }
        return executeSession;
    }

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
            } catch (SQLException | SessionException e) {
                log.error("Error creating connections pools", e);
//                throw new RuntimeException(e);
            }
        }
        return connectionPools;
    }

    private ConnectionPools getConnectionPoolsImpl() throws SQLException, SessionException {
        ConnectionPools rtn = null;
        HmsMirrorConfig config = getExecuteSession().getConfig();

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

//    public boolean reset() throws SQLException, EncryptionException, SessionException {

    /// /        close();
//        boolean rtn = init();
//        return rtn;
//    }
    public boolean init() throws SQLException, SessionException, EncryptionException, URISyntaxException {
        // Set Session Connections.
        boolean rtn = Boolean.TRUE;
        ExecuteSession executeSession = getExecuteSession();

        if (executeSession.isConnected()) {
            log.info("Connections already established.  Skipping Connection Setup/Validation.");
            return Boolean.TRUE;
        }

//        if (executeSession.isRunning()) {
//            throw new SessionException("Session is already running.  Can't validate connections at this time.");
//        }

        RunStatus runStatus = executeSession.getRunStatus();
        runStatus.setProgress(ProgressEnum.STARTED);
        this.close();

        initConnectionsSetup();

        // Initialize the drivers
//        getConnectionPools().init();

        rtn = initHS2();
        boolean msRtn = initMetastoreDirect();
        boolean nsRtn = initHcfsNamespaces();

        if (rtn && msRtn && nsRtn) {
//            runStatus.setProgress(ProgressEnum.COMPLETED);
            executeSession.setConnected(Boolean.TRUE);
        } else {
            runStatus.setProgress(ProgressEnum.FAILED);
        }

        // Ensure they werefdfasd
        return rtn && msRtn && nsRtn;
    }

    protected boolean initHcfsNamespaces() throws SessionException {
        AtomicBoolean rtn = new AtomicBoolean(Boolean.TRUE);
        ExecuteSession session = getExecuteSession();
        Connections connections = session.getConnections();
        // TODO: Make sure connections has been populated.

        HmsMirrorConfig config = session.getConfig();

        config.getClusters().forEach((k, v) -> {
            // checked..
            if (!isBlank(v.getHcfsNamespace())) {
                connections.getNamespaces().get(k).setEndpoint(v.getHcfsNamespace());
                try {
                    log.info("Testing HCFS Connection for {}", k);
                    CommandReturn cr = cliEnvironment.processInput("ls /");
                    if (cr.isError()) {
                        log.error("HCFS Connection Failed for {} with: {}", k, cr.getError());
                        connections.getNamespaces().get(k).setStatus(ConnectionStatus.FAILED);
                        connections.getNamespaces().get(k).setMessage(cr.getError());
                        rtn.set(Boolean.FALSE);
                    } else {
                        log.info("HCFS Connection Successful for {}", k);
                        connections.getNamespaces().get(k).setStatus(ConnectionStatus.SUCCESS);
                    }
                } catch (DisabledException e) {
                    log.info("HCFS Connection Disabled for {}", k);
                    connections.getNamespaces().get(k).setStatus(ConnectionStatus.DISABLED);
//                        throw new RuntimeException(e);
                }
            } else {
                log.info("HCFS Connection Not Configured for {}", k);
                connections.getNamespaces().get(k).setStatus(ConnectionStatus.NOT_CONFIGURED);
            }
        });
        return rtn.get();
    }

    public boolean initMetastoreDirectOnly() throws SQLException, SessionException, EncryptionException, URISyntaxException {
        initConnectionsSetup();
        boolean rtn = initMetastoreDirect();
        return rtn;
    }

    protected boolean initMetastoreDirect() throws SQLException, SessionException, EncryptionException {
        AtomicBoolean rtn = new AtomicBoolean(Boolean.TRUE);
        ExecuteSession session = getExecuteSession();
        Connections connections = session.getConnections();
        // TODO: Make sure connections has been populated.

        HmsMirrorConfig config = session.getConfig();

        config.getClusters().forEach((k, v) -> {
            if (!isNull(v.getMetastoreDirect())) { // && !finalConfigErrors) {
                connections.getMetastoreDirectConnections().get(k).setEndpoint(v.getMetastoreDirect().getUri());
                if (!configService.isMetastoreDirectConfigured(session, k)) {
                    connections.getMetastoreDirectConnections().get(k).setStatus(ConnectionStatus.NOT_CONFIGURED);
                    connections.getMetastoreDirectConnections().get(k).setMessage(MessageCode.LEFT_METASTORE_URI_NOT_DEFINED.getDesc());
                } else {
                    try {
                        log.info("Testing Metastore Direct Connection for {}", k);
                        Connection conn = getConnectionPools().getMetastoreDirectEnvironmentConnection(k);
                        if (conn != null) {
                            log.info("Metastore Direct Connection Successful for {}", k);
                        } else {
                            log.error("Metastore Direct Connection Failed for {}", k);
                            rtn.set(Boolean.FALSE);
                        }
                        connections.getMetastoreDirectConnections().get(k).setStatus(ConnectionStatus.SUCCESS);
                    } catch (SQLException se) {
                        log.error("Metastore Direct Connection Failed for {}", k, se);
                        connections.getMetastoreDirectConnections().get(k).setStatus(ConnectionStatus.FAILED);
                        connections.getMetastoreDirectConnections().get(k).setMessage(se.getMessage());
                    }
                }

            } else if (!isNull(v.getMetastoreDirect())) {
                log.info("Metastore Direct Connection Check Configuration for {}", k);
                connections.getMetastoreDirectConnections().get(k).setStatus(ConnectionStatus.CHECK_CONFIGURATION);
            } else {
                log.info("Metastore Direct Connection Not Configured for {}", k);
                connections.getMetastoreDirectConnections().get(k).setStatus(ConnectionStatus.NOT_CONFIGURED);
            }
        });
        return rtn.get();
    }

    protected void initConnectionsSetup() throws SQLException, SessionException, EncryptionException, URISyntaxException {
        // Close and reset the connections.

        ExecuteSession session = getExecuteSession();
        Connections connections = session.getConnections();
        connections.reset();

        HmsMirrorConfig config = session.getConfig();

//        RunStatus runStatus = executeSession.getRunStatus();

        if (config.getDataStrategy() == DataStrategyEnum.DUMP) {
            config.setExecute(Boolean.FALSE); // No Actions.
            config.setSync(Boolean.FALSE);
        }

        // Make adjustments to the config clusters based on settings.
        // Buildout the right connections pool details.
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
                connections.getHiveServer2Connections().get(Environment.LEFT)
                        .setEndpoint(config.getCluster(Environment.LEFT)
                                .getHiveServer2().getUri());
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
                    // Place holder for the right connection to capture element for reporting.
                    connections.getHiveServer2Connections().get(Environment.RIGHT)
                            .setEndpoint(config.getCluster(Environment.RIGHT)
                                    .getHiveServer2().getUri());
                }
            default:
                getConnectionPools().addHiveServer2(Environment.LEFT, config.getCluster(Environment.LEFT).getHiveServer2());
                connections.getHiveServer2Connections().get(Environment.LEFT)
                        .setEndpoint(config.getCluster(Environment.LEFT)
                                .getHiveServer2().getUri());
                break;
        }
        if (nonNull(config.getCluster(Environment.LEFT)) &&
                nonNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
            getConnectionPools().addMetastoreDirect(Environment.LEFT, config.getCluster(Environment.LEFT).getMetastoreDirect());
            connections.getMetastoreDirectConnections().get(Environment.LEFT)
                    .setEndpoint(config.getCluster(Environment.LEFT)
                            .getMetastoreDirect().getUri());
        }
        if (nonNull(config.getCluster(Environment.RIGHT)) &&
                nonNull(config.getCluster(Environment.RIGHT).getMetastoreDirect())) {
            getConnectionPools().addMetastoreDirect(Environment.RIGHT, config.getCluster(Environment.RIGHT).getMetastoreDirect());
            connections.getMetastoreDirectConnections().get(Environment.RIGHT)
                    .setEndpoint(config.getCluster(Environment.RIGHT)
                            .getMetastoreDirect().getUri());
        }

        if (!isNull(config.getCluster(Environment.LEFT)) &&
                !isBlank(config.getCluster(Environment.LEFT).getHcfsNamespace())) {
            connections.getNamespaces().get(Environment.LEFT)
                    .setEndpoint(config.getCluster(Environment.LEFT)
                            .getHcfsNamespace());
        }

        if (!isNull(config.getCluster(Environment.RIGHT)) &&
                !isBlank(config.getCluster(Environment.RIGHT).getHcfsNamespace())) {
            connections.getNamespaces().get(Environment.RIGHT)
                    .setEndpoint(config.getCluster(Environment.RIGHT)
                            .getHcfsNamespace());
        }

        if (!isNull(config.getTransfer().getTargetNamespace()) &&
                !isBlank(config.getTransfer().getTargetNamespace())) {
            connections.getNamespaces().get(Environment.TARGET)
                    .setEndpoint(config.getTransfer().getTargetNamespace());
        }
        // Should've been called already and HMSMirrorAppService.
        environmentService.setupGSS();

        // Initialize the drivers
        getConnectionPools().init();

    }

    //    @Override
    protected boolean initHS2() throws SQLException, SessionException, EncryptionException {
        boolean rtn = Boolean.TRUE;
        ExecuteSession session = getExecuteSession();
        Connections connections = session.getConnections();

        HmsMirrorConfig config = session.getConfig();

        RunStatus runStatus = executeSession.getRunStatus();

        try {
            Environment[] hs2Envs = {Environment.LEFT, Environment.RIGHT};
            for (Environment target : hs2Envs) {
                Connection conn = null;
                Statement stmt = null;
                try {
                    log.info("Testing HiveServer2 Connection for {}", target);
                    conn = getConnectionPools().getHS2EnvironmentConnection(target);
                    if (isNull(conn)) {
                        if (isNull(config.getCluster(target))
                                || isNull(config.getCluster(target).getHiveServer2())) {
                            runStatus.addWarning(ENVIRONMENT_DISCONNECTED, target);
                        } else if (target == Environment.RIGHT
                                && config.getCluster(target).getHiveServer2().isDisconnected()) {
                            // Skip error.  Set Warning that we're disconnected.
                            runStatus.addWarning(ENVIRONMENT_DISCONNECTED, target);
                        } else if (!config.isLoadingTestData()) {
                            runStatus.addError(ENVIRONMENT_CONNECTION_ISSUE, target);
                            connections.getHiveServer2Connections().get(target).setStatus(ConnectionStatus.FAILED);
                            connections.getHiveServer2Connections().get(target).setMessage("Connection is null.");
                            rtn = Boolean.FALSE;
                        }
                    } else {
                        // Exercise the connections.
                        log.info("HS2 Connection Successful for {}", target);
                        stmt = conn.createStatement();

                        // Run these first to ensure we preset the queue, if being set.
                        // Property Overrides from 'config.optimization.overrides'
                        List<String> overrides = ConfigUtils.getPropertyOverridesFor(target, config);
                        connections.getHiveServer2Connections().get(target).setProperties(overrides);
                        // Run the overrides;
                        for (String o : overrides) {
                            log.info("Running Override: {} on {} connection", o, target);
                            stmt.execute(o);
                        }

                        // Create an array of strings with various settings to run.
                        String[] sessionSets = {
                                "SET hive.query.results.cache.enabled=false",
                                "SET hive.fetch.task.conversion = none",
                                "SELECT 1"
                        };
                        for (String s : sessionSets) {
                            connections.getHiveServer2Connections().get(target).getProperties().add(s);
                            log.info("Running session check: {} on {} connection", s, target);
                            stmt.execute(s);
                        }

                        log.info("HS2 Connection validated (resources) for {}", target);
                        connections.getHiveServer2Connections().get(target).setStatus(ConnectionStatus.SUCCESS);
                        connections.getHiveServer2Connections().get(target).setMessage("Connection Successful and Validated.");
                    }
                } catch (SQLException se) {
                    if (target == Environment.RIGHT && config.getCluster(target).getHiveServer2().isDisconnected()) {
                        // Set warning that RIGHT is disconnected.
                        runStatus.addWarning(ENVIRONMENT_DISCONNECTED, target);
                    } else {
                        log.error(se.getMessage(), se);
                        runStatus.addError(ENVIRONMENT_CONNECTION_ISSUE, target, se.getMessage());
                        connections.getHiveServer2Connections().get(target).setStatus(ConnectionStatus.FAILED);
                        connections.getHiveServer2Connections().get(target).setMessage(se.getMessage());
                        rtn = Boolean.FALSE;
                    }
                } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                    runStatus.addError(ENVIRONMENT_CONNECTION_ISSUE, target, t.getMessage());
                    connections.getHiveServer2Connections().get(target).setStatus(ConnectionStatus.FAILED);
                    connections.getHiveServer2Connections().get(target).setMessage(t.getMessage());
                    rtn = Boolean.FALSE;
                } finally {
                    if (nonNull(stmt)) {
                        stmt.close();
                    }
                    if (nonNull(conn)) {
                        conn.close();
                    }
                }
            }
        } catch (SQLException cnfe) { // | URISyntaxException cnfe) {
            getConnectionPools().close();
            log.error("Issue initializing connections.  Check driver locations", cnfe);
            throw new RuntimeException(cnfe);
        }

        // Set state of connections.
        connected = rtn;
        return rtn;
    }

}
