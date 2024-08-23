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

package com.cloudera.utils.hms.mirror.web.controller;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.ConnectionStatus;
import com.cloudera.utils.hms.mirror.domain.support.Connections;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ConnectionPoolService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.UIModelService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Controller
@RequestMapping(path = "/connections")
@Slf4j
public class ConnectionMVController {

    private ConfigService configService;
    private ConnectionPoolService connectionPoolService;
    private ExecuteSessionService executeSessionService;
    private UIModelService uiModelService;
    private CliEnvironment cliEnvironment;

    @Autowired
    public void setCliEnvironment(CliEnvironment cliEnvironment) {
        this.cliEnvironment = cliEnvironment;
    }

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setUiModelService(UIModelService uiModelService) {
        this.uiModelService = uiModelService;
    }

    @RequestMapping(value = "/validate", method = RequestMethod.GET)
    public String validate(Model model) {
        uiModelService.sessionToModel(model, 1, false);
        return "connections/validate";
    }

    @RequestMapping(value = "/doValidate", method = RequestMethod.POST)
    public String doValidate(Model model) throws SessionException, EncryptionException {

        executeSessionService.closeSession();

        ExecuteSession session = executeSessionService.getSession();
        Connections connections = session.getConnections();

        HmsMirrorConfig config = session.getConfig();
        boolean configErrors = !configService.validateForConnections(session);
        if (!configErrors) {
            try {
                executeSessionService.startSession(1);
                log.info("Initializing Connection Pools");
                connectionPoolService.init();
                log.info("Connection Pools Initialized");
            } catch (SQLException e) {
                log.error("Error initializing connection pools", e);
                configErrors = Boolean.TRUE;
            }
        }

        boolean finalConfigErrors = configErrors;
        config.getClusters().forEach((k, v) -> {
            if (nonNull(v)) {
                if (nonNull(v.getHiveServer2()) && !finalConfigErrors) {
                    try {
                        log.info("Testing HiveServer2 Connection for {}", k);
                        Connection conn = connectionPoolService.getConnectionPools().getHS2EnvironmentConnection(k);
                        if (conn != null) {
                            log.info("HS2 Connection Successful for {}", k);
                        } else {
                            log.error("HS2 Connection Failed for {}", k);
                        }
                        connections.getHiveServer2Connections().get(k).setStatus(ConnectionStatus.SUCCESS);
                    } catch (SQLException se) {
                        log.error("HS2 Connection Failed for {}", k, se);
                        connections.getHiveServer2Connections().get(k).setStatus(ConnectionStatus.FAILED);
                        connections.getHiveServer2Connections().get(k).setMessage(se.getMessage());
                    }
                } else if (nonNull(v.getHiveServer2())) {
                    log.info("HS2 Connection Check Configuration for {}", k);
                    connections.getHiveServer2Connections().get(k).setStatus(ConnectionStatus.CHECK_CONFIGURATION);
                } else {
                    log.info("HS2 Connection Not Configured for {}", k);
                    connections.getHiveServer2Connections().get(k).setStatus(ConnectionStatus.NOT_CONFIGURED);
                }

                if (nonNull(v.getMetastoreDirect()) && !finalConfigErrors) {
                    try {
                        log.info("Testing Metastore Direct Connection for {}", k);
                        Connection conn = connectionPoolService.getConnectionPools().getMetastoreDirectEnvironmentConnection(k);
                        if (conn != null) {
                            log.info("Metastore Direct Connection Successful for {}", k);
                        } else {
                            log.error("Metastore Direct Connection Failed for {}", k);
                        }
                        connections.getMetastoreDirectConnections().get(k).setStatus(ConnectionStatus.SUCCESS);
                    } catch (SQLException se) {
                        log.error("Metastore Direct Connection Failed for {}", k, se);
                        connections.getMetastoreDirectConnections().get(k).setStatus(ConnectionStatus.FAILED);
                        connections.getMetastoreDirectConnections().get(k).setMessage(se.getMessage());
                    }

                } else if (nonNull(v.getMetastoreDirect())) {
                    log.info("Metastore Direct Connection Check Configuration for {}", k);
                    connections.getMetastoreDirectConnections().get(k).setStatus(ConnectionStatus.CHECK_CONFIGURATION);
                } else {
                    log.info("Metastore Direct Connection Not Configured for {}", k);
                    connections.getMetastoreDirectConnections().get(k).setStatus(ConnectionStatus.NOT_CONFIGURED);
                }
                // checked..
                if (!isBlank(v.getHcfsNamespace())) {
                    try {
                        log.info("Testing HCFS Connection for {}", k);
                        CommandReturn cr = cliEnvironment.processInput("ls /");
                        if (cr.isError()) {
                            log.error("HCFS Connection Failed for {} with: {}", k, cr.getError());
                            connections.getNamespaces().get(k).setStatus(ConnectionStatus.FAILED);
                            connections.getNamespaces().get(k).setMessage(cr.getError());
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
            }
        });
        try {
            if (!config.isSkipLinkCheck() && !isBlank(config.getTargetNamespace())) {
                try {
                    CommandReturn cr = cliEnvironment.processInput("ls " + config.getTargetNamespace());
                    if (cr.isError()) {
                        connections.getNamespaces().get(Environment.TARGET).setStatus(ConnectionStatus.FAILED);
                        connections.getNamespaces().get(Environment.TARGET).setMessage(cr.getError());
                    } else {
                        connections.getNamespaces().get(Environment.TARGET).setStatus(ConnectionStatus.SUCCESS);
                    }
                } catch (DisabledException e) {
                    connections.getNamespaces().get(Environment.TARGET).setStatus(ConnectionStatus.DISABLED);
//                        throw new RuntimeException(e);
                } catch (RequiredConfigurationException e) {
                    connections.getNamespaces().get(Environment.TARGET).setStatus(ConnectionStatus.DISABLED);
                }

            }
        } catch (RequiredConfigurationException e) {
            connections.getNamespaces().get(Environment.TARGET).setStatus(ConnectionStatus.NOT_CONFIGURED);
        }

        uiModelService.sessionToModel(model, 1, false);

        return "connections/validate";
    }
}
