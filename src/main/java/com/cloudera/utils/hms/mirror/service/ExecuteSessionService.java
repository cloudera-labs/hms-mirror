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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.jcabi.manifests.Manifests;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.cloudera.utils.hms.mirror.MessageCode.ENCRYPTED_PASSWORD_CHANGE_ATTEMPT;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Slf4j
@Getter
public class ExecuteSessionService {

    public static final String DEFAULT = "default.yaml";

    private final CliEnvironment cliEnvironment;
    private final ConfigService configService;
    private final ConnectionPoolService connectionPoolService;

    /*
    This is the current session that can be modified (but not running yet).  This is where
    configurations can be changed before running.  Once the session is kicked off, the object
    should be cloned and the original session moved to the 'activeSession' field and added
    to the 'executeSessionQueue'.
     */
    @Setter
    private ExecuteSession session;

    private String reportOutputDirectory;

    private boolean amendSessionIdToReportDir = Boolean.TRUE;

    /**
     * Used to limit the number of sessionHistory that are retained in memory.
     */
    private final Map<String, ExecuteSession> sessionHistory = new HashMap<>();

    /**
     * Constructor for ExecuteSessionService.
     *
     * @param configService Service for configuration
     * @param cliEnvironment CLI environment
     * @param connectionPoolService Service for managing connection pools
     */
    public ExecuteSessionService(ConfigService configService, 
                                CliEnvironment cliEnvironment, 
                                ConnectionPoolService connectionPoolService) {
        this.configService = configService;
        this.cliEnvironment = cliEnvironment;
        this.connectionPoolService = connectionPoolService;
        log.debug("ExecuteSessionService initialized");
    }

    public void setReportOutputDirectory(String reportOutputDirectory, boolean amendSessionIdToReportDir) {
        this.amendSessionIdToReportDir = amendSessionIdToReportDir;
        this.reportOutputDirectory = reportOutputDirectory;
    }

    /**
     * Saves the provided HmsMirrorConfig instance after merging it with the current configuration.
     * Adjusts connection settings and applies configuration rules, ensuring the
     * correct management of encrypted and non-encrypted password changes as well
     * as property overrides. Throws an exception if the session is currently running.
     *
     * @param config The HmsMirrorConfig object containing the configuration to be saved.
     *               This configuration will be merged with the existing configuration
     *               of the associated session.
     * @param maxThreads The Thread used by the application at the time of the report.
     * @return {@code true} if the save operation is successful, or {@code false}
     *         if there were issues such as an attempted encrypted password change.
     * @throws SessionException If the session is running or encounters a problem during the save operation.
     */
    public boolean save(HmsMirrorConfig config, int maxThreads) throws SessionException {
        boolean rtn = Boolean.TRUE;
        AtomicReference<Boolean> passwordCheck = new AtomicReference<>(Boolean.FALSE);

        ExecuteSession session = getSession();

        // Reset the connection status.
        session.setConnected(Boolean.FALSE);

        if (session.isRunning()) {
            throw new SessionException("Can't save while running.");
        } else {
            session.getRunStatus().reset();
        }

        HmsMirrorConfig currentConfig = session.getConfig();

        // Reload Databases
        config.getDatabases().addAll(currentConfig.getDatabases());

        // Merge Passwords
        config.getClusters().forEach((env, cluster) -> {
            // HS2
            if (nonNull(cluster.getHiveServer2())) {
                String currentPassword = (String) currentConfig.getClusters().get(env).getHiveServer2().getConnectionProperties().get("password");
                String newPassword = (String) cluster.getHiveServer2().getConnectionProperties().get("password");
                if (newPassword != null && !newPassword.isEmpty()) {
                    // Set new Password, IF the current passwords aren't ENCRYPTED...  set warning if they attempted.
                    if (config.isEncryptedPasswords()) {
                        // Restore original password
                        cluster.getHiveServer2().getConnectionProperties().put("password", currentPassword);
                        passwordCheck.set(Boolean.TRUE);
                    } else {
                        cluster.getHiveServer2().getConnectionProperties().put("password", newPassword);
                    }
                } else if (currentPassword != null) {
                    // Restore original password
                    cluster.getHiveServer2().getConnectionProperties().put("password", currentPassword);
                } else {
                    cluster.getHiveServer2().getConnectionProperties().remove("password");
                }
            }

            // Metastore
            if (nonNull(cluster.getMetastoreDirect())) {
                String currentPassword = (String) currentConfig.getClusters().get(env).getMetastoreDirect().getConnectionProperties().get("password");
                String newPassword = (String) cluster.getMetastoreDirect().getConnectionProperties().get("password");
                if (newPassword != null && !newPassword.isEmpty()) {
                    // Set new password
                    if (config.isEncryptedPasswords()) {
                        // Restore original password
                        cluster.getHiveServer2().getConnectionProperties().put("password", currentPassword);
                        passwordCheck.set(Boolean.TRUE);
                    } else {
                        cluster.getMetastoreDirect().getConnectionProperties().put("password", newPassword);
                    }
                } else if (currentPassword != null) {
                    // Restore Original password
                    cluster.getMetastoreDirect().getConnectionProperties().put("password", currentPassword);
                } else {
                    cluster.getMetastoreDirect().getConnectionProperties().remove("password");
                }
            }
        });

        // Merge Translator
        config.setTranslator(currentConfig.getTranslator());

        // Merge the Property Overrides
        config.getOptimization().setOverrides(currentConfig.getOptimization().getOverrides());

        // Apply rules for the DataStrategy that are not in the config.
        configService.alignConfigurationSettings(session, config);

        // Reset to the merged config.
        session.setConfig(config);

//        model.addAttribute(READ_ONLY, Boolean.TRUE);
        configService.validate(session, null);

        if (passwordCheck.get()) {
            ExecuteSession session1 = getSession();
            session1.getRunStatus().addError(ENCRYPTED_PASSWORD_CHANGE_ATTEMPT);
            rtn = Boolean.FALSE;
        }

        return rtn;
    }

    public ExecuteSession createSession(String sessionId, HmsMirrorConfig hmsMirrorConfig) {
        String sessionName = !isBlank(sessionId) ? sessionId : DEFAULT;
        
        ExecuteSession session = new ExecuteSession();
        session.setSessionId(sessionName);
        session.setConfig(hmsMirrorConfig);
        return session;
    }

    public void closeSession() throws SessionException {
        // No-op for now, but keeping method for future implementation
    }

    public ExecuteSession getSession() {
        return session;
    }

    public ExecuteSession getSession(String sessionId) {
        if (isBlank(sessionId)) {
            if (isNull(session)) {
                log.error("No session loaded");
                return null;
            }
            return session;
        }
        
        if (sessionHistory.containsKey(sessionId)) {
            return sessionHistory.get(sessionId);
        }
        
        log.error("Session not found: {}", sessionId);
        return null;
    }

    /*
      Look at the 'activeSession' and if it is not null, check that it is not running.
        If it is not running, then clone the session and add it to the 'executeSessionQueue'.
        Set the 'activeSession' to null.  The 'getActiveSession' will then return the last session
        placed in the queue and set 'activeSession' to that session.

        This allow us to keep the current and active sessionHistory separate.  The active session is the
        one that will be referenced during the run.
     */
    public Boolean startSession(Integer concurrency) throws SessionException {
        Boolean rtn = Boolean.TRUE;

        ExecuteSession session = getSession();
        // Set the concurrency.
        session.setConcurrency(concurrency);

        if (!isNull(session) && session.isRunning()) {
            throw new SessionException("Session is still running.  Cannot transition to active.");
        }

        // This should get the loaded session and clone it.
        if (isNull(session)) {
            throw new SessionException("No session loaded.");
        }

        // Will create new RunStatus and set version info.
        RunStatus runStatus = new RunStatus();
        runStatus.setConcurrency(concurrency);
        // Link the RunStatus to the session so users know what session details to retrieve.
        runStatus.setSessionId(session.getSessionId());
        runStatus.setProgress(ProgressEnum.STARTED);

        // Reset for each transition.
        // Set the active session id to the current date and time.
        DateFormat dtf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        session.setSessionId(dtf.format(new Date()));

        // If it's connected (Active Session), don't go through all this again.
        log.debug("Configure and setup Session");
        HmsMirrorConfig config = session.getConfig();

        // TODO: Set Metastore Direct Concurrency.

        // Connection Service should be set to the resolved config.
        connectionPoolService.setExecuteSession(session);

        try {
            runStatus.setAppVersion(Manifests.read("HMS-Mirror-Version"));
        } catch (IllegalArgumentException iae) {
            runStatus.setAppVersion("Unknown");
        }
        // Set/Reset the run status.
        session.setRunStatus(runStatus);

        // New Conversion object for each run.
        session.setConversion(new Conversion());

        return rtn;
    }

}
