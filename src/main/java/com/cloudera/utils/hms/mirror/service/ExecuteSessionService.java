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
import org.springframework.beans.factory.annotation.Autowired;
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

    private CliEnvironment cliEnvironment;
    private ConfigService configService;
    private ConnectionPoolService connectionPoolService;

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

    /*
    Used to limit the number of sessionHistory that are retained in memory.
     */
//    private int maxRetainedSessions = 5;
//    private final Map<String, ExecuteSession> executeSessionMap = new TreeMap<>();
    private final Map<String, ExecuteSession> sessionHistory = new HashMap<>();

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setCliEnvironment(CliEnvironment cliEnvironment) {
        this.cliEnvironment = cliEnvironment;
    }

    @Autowired
    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }

    public void setReportOutputDirectory(String reportOutputDirectory, boolean amendSessionIdToReportDir) {
        this.amendSessionIdToReportDir = amendSessionIdToReportDir;
        this.reportOutputDirectory = reportOutputDirectory;
    }

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
//
        ExecuteSession session;
//        if (sessionHistory.containsKey(sessionName)) {
//            session = sessionHistory.get(sessionName);
//            session.setConfig(hmsMirrorConfig);
//        } else {
        session = new ExecuteSession();
        session.setSessionId(sessionName);
        session.setConfig(hmsMirrorConfig);
//        sessionHistory.put(sessionName, session);
//        }
        return session;
    }

//    public void clear2Session() throws SessionException {
//        closeSession();
//        session = null;
//    }

    public void closeSession() throws SessionException {
//        if (nonNull(session)) {
//            if (session.isRunning()) {
//                throw new SessionException("Session is still running.  You can't change the session while it is running.");
//            }
//        }
    }

    public ExecuteSession getSession() {
        return session;
    }

    /*
    If sessionId is null, then pull the 'current' session.
    If sessionId is NOT null, look for it in the session map and return it.
    When not found, throw exception.
     */
    public ExecuteSession getSession(String sessionId) {
        if (isBlank(sessionId)) {
            if (isNull(session)) {
                log.error("No session loaded");
                return null;
//                throw new RuntimeException("No session loaded.");
            }
            return session;
        } else {
            if (sessionHistory.containsKey(sessionId)) {
                return sessionHistory.get(sessionId);
            } else {
                log.error("Session not found: " + sessionId);
                return null;
            }
        }
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

        // Throws Exception if can't close (running).
//        closeSession();

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

        // Ensure the configuration is valid.
//        if (!configService.validate(session, null)) {
//            runStatus.setProgress(ProgressEnum.FAILED);
//            runStatus.addError(MessageCode.CONFIG_INVALID);
//        };

        // Reset for each transition.
        // Set the active session id to the current date and time.
        DateFormat dtf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        session.setSessionId(dtf.format(new Date()));

        // If it's connected (Active Session), don't go through all this again.
//        if (isNull(session) || !session.isConnected()) {
        log.debug("Configure and setup Session");
        HmsMirrorConfig config = session.getConfig();

        // Setup connections concurrency
        // We need to pass on a few scale parameters to the hs2 configs so the connections pools can handle the scale requested.
        if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getHiveServer2()) && nonNull(concurrency)) {
            Cluster cluster = config.getCluster(Environment.LEFT);
            cluster.getHiveServer2().getConnectionProperties().setProperty("initialSize", Integer.toString(concurrency / 2));
            cluster.getHiveServer2().getConnectionProperties().setProperty("minIdle", Integer.toString(concurrency / 2));
            if (cluster.getHiveServer2().getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
                cluster.getHiveServer2().getConnectionProperties().setProperty("maxIdle", Integer.toString(concurrency));
                if (isNull(cluster.getHiveServer2().getConnectionProperties().getProperty("maxWaitMillis")) ||
                        isBlank(cluster.getHiveServer2().getConnectionProperties().getProperty("maxWaitMillis"))) {
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxWaitMillis", "5000");
                }
                cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", Integer.toString(concurrency));
            }
        }
        if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getHiveServer2()) && nonNull(concurrency)) {
            Cluster cluster = config.getCluster(Environment.RIGHT);
            cluster.getHiveServer2().getConnectionProperties().setProperty("initialSize", Integer.toString(concurrency / 2));
            cluster.getHiveServer2().getConnectionProperties().setProperty("minIdle", Integer.toString(concurrency / 2));
            if (cluster.getHiveServer2().getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
                cluster.getHiveServer2().getConnectionProperties().setProperty("maxIdle", Integer.toString(concurrency));
                if (isNull(cluster.getHiveServer2().getConnectionProperties().getProperty("maxWaitMillis")) ||
                        isBlank(cluster.getHiveServer2().getConnectionProperties().getProperty("maxWaitMillis"))) {
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxWaitMillis", "5000");
                }
                cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", Integer.toString(concurrency));
            }
        }

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
