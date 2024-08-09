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

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.Translator;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.PersistContainer;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.mirror.util.ModelUtils;
import com.cloudera.utils.hms.mirror.web.service.WebConfigService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.cloudera.utils.hms.mirror.MessageCode.ENCRYPTED_PASSWORD_CHANGE_ATTEMPT;
import static java.util.Objects.nonNull;

@Controller
@RequestMapping(path = "/config")
@Slf4j
public class ConfigMVController implements ControllerReferences {

    private ConfigService configService;
    private ExecuteSessionService executeSessionService;
    private PasswordService passwordService;
    private ReportService reportService;
    private WebConfigService webConfigService;
    private DatabaseService databaseService;
    private UIModelService uiModelService;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setDatabaseService(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setPasswordService(PasswordService passwordService) {
        this.passwordService = passwordService;
    }

    @Autowired
    public void setReportService(ReportService reportService) {
        this.reportService = reportService;
    }

    @Autowired
    public void setWebConfigService(WebConfigService webConfigService) {
        this.webConfigService = webConfigService;
    }

    @Autowired
    public void setUiModelService(UIModelService uiModelService) {
        this.uiModelService = uiModelService;
    }

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String index(Model model,
                        @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) {
        uiModelService.sessionToModel(model, maxThreads, null);
        return "index";
    }

    @RequestMapping(value = "/home", method = RequestMethod.GET)
    public String home(Model model,
                       @Value("${hms-mirror.config.testing}") Boolean testing,
                       @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) {
//        model.addAttribute(ACTION, "home");

//        ExecuteSession executeSession = executeSessionService.getCurrentSession();

        // Get list of available configs
        Set<String> configs = webConfigService.getConfigList();
        model.addAttribute(CONFIG_LIST, configs);

        uiModelService.sessionToModel(model, maxThreads, testing);

        // Get list of Reports
        model.addAttribute(REPORT_LIST, reportService.getAvailableReports());

        return "home";
    }

    @RequestMapping(value = "/init", method = RequestMethod.GET)
    public String init(Model model,
                       @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException {
        model.addAttribute(ACTION, "init");
        // Clear the loaded and active session.
//        executeSessionService.clearLoadedSession();
        // Create new Session (with blank config)
//        HmsMirrorConfig config = configService.createForDataStrategy(DataStrategyEnum.valueOf(dataStrategy));
//        ExecuteSession session = executeSessionService.createSession(NEW_CONFIG, config);
        // Set the Loaded Session
//        executeSessionService.setCurrentSession(session);
//        // Setup Model for MVC
//        model.addAttribute(CONFIG, session.getConfig());
//        model.addAttribute(SESSION_ID, session.getSessionId());

        // Get list of available configs
        Set<String> configs = webConfigService.getConfigList();
        model.addAttribute(CONFIG_LIST, configs);

        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);

        return "config/init";
    }

    @RequestMapping(value = "/doCreate", method = RequestMethod.POST)
    public String doCreate(Model model,
                           @RequestParam(value = DATA_STRATEGY, required = true) String dataStrategy,
                           @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException {
        model.addAttribute(READ_ONLY, Boolean.FALSE);
        // Clear the loaded and active session.
        executeSessionService.clearSession();
        // Create new Session (with blank config)
        HmsMirrorConfig config = configService.createForDataStrategy(DataStrategyEnum.valueOf(dataStrategy));
        ExecuteSession session = executeSessionService.createSession(NEW_CONFIG, config);
        // Set the Loaded Session
        executeSessionService.setCurrentSession(session);
//        // Setup Model for MVC
//        model.addAttribute(CONFIG, session.getConfig());
//        model.addAttribute(SESSION_ID, session.getSessionId());

        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);

        return "config/view";
    }


    @RequestMapping(value = "/edit", method = RequestMethod.GET)
    public String edit(Model model,
                       @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException {
//        executeSessionService.clearActiveSession();
//        model.addAttribute(ACTION, "edit");
        model.addAttribute(READ_ONLY, Boolean.FALSE);
        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);
        return "config/view";
    }

    @RequestMapping(value = "/doSave", method = RequestMethod.POST)
    public String doSave(Model model,
                         @ModelAttribute(CONFIG) HmsMirrorConfig config,
//                         @PathVariable @NotNull ConfigSection section,
                         @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException {
        executeSessionService.clearActiveSession();

        AtomicReference<Boolean> passwordCheck = new AtomicReference<>(Boolean.FALSE);

        ExecuteSession session = executeSessionService.getSession();

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

        // Apply rules for the DataStrategy that are not in the config.
        configService.alignConfigurationSettings(session, config);

        // Reset to the merged config.
        session.setConfig(config);

        model.addAttribute(READ_ONLY, Boolean.TRUE);
        configService.validate(session, null);

//        executeSessionService.transitionLoadedSessionToActive(maxThreads);
        // After a 'save', the session connections statuses should be reset.
        session.resetConnectionStatuses();

        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);

        if (passwordCheck.get()) {
            ExecuteSession session1 = executeSessionService.getSession();
            session1.getRunStatus().addError(ENCRYPTED_PASSWORD_CHANGE_ATTEMPT);
            return "error";
        }

        return "config/view";
    }


    @RequestMapping(value = "/persist", method = RequestMethod.GET)
    public String persist(Model model,
                          @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws IOException, SessionException {

        uiModelService.sessionToModel(model, maxThreads, false);

        return "config/persist";
    }

    @RequestMapping(value = "/doPersist", method = RequestMethod.POST)
    public String doPersist(Model model,
                            @Value("${hms-mirror.config.path}") String configPath,
                            @ModelAttribute(PERSIST) PersistContainer persistContainer,
                            @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws IOException, SessionException {
        // Get the current session config.
        executeSessionService.clearActiveSession();

        ExecuteSession curSession = executeSessionService.getSession();
        HmsMirrorConfig currentConfig = curSession.getConfig();

        if (persistContainer.isFlipConfigs()) {
            configService.flipConfig(currentConfig);
        }

        // Clone and save clone
        HmsMirrorConfig config = currentConfig.clone();
        if (persistContainer.isStripMappings()) {
            config.setTranslator(new Translator());
            config.getDatabases().clear();
        }

        String saveAs = null;
        if (persistContainer.isSaveAsDefault()) {
            log.info("Saving Config as Default: {}", DEFAULT_CONFIG);
            saveAs = DEFAULT_CONFIG;
            String configFullFilename = configPath + File.separator + saveAs;
            configService.saveConfig(config, configFullFilename, Boolean.TRUE);
        }
        if (!persistContainer.getSaveAs().isEmpty()) {
            curSession.setSessionId(persistContainer.getSaveAs());
            if (!(persistContainer.getSaveAs().contains(".yaml") || persistContainer.getSaveAs().contains(".yml"))) {
                persistContainer.setSaveAs(persistContainer.getSaveAs() + ".yaml");//saveAs += ".yaml";
            }
            log.info("Saving Config as: {}", persistContainer.getSaveAs());
            String configFullFilename = configPath + File.separator + persistContainer.getSaveAs();
            configService.saveConfig(config, configFullFilename, Boolean.TRUE);
        }

        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);
        model.addAttribute(ACTION, "view");
        model.addAttribute(READ_ONLY, Boolean.TRUE);

        ModelUtils.allEnumsForMap(currentConfig.getDataStrategy(), model.asMap());

        return "config/view";
    }

    @RequestMapping(value = "/doReload", method = RequestMethod.POST)
    public String doReload(Model model,
                           @RequestParam(value = SESSION_ID, required = true) String sessionId,
                           @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException {
        // Don't reload if running.
        executeSessionService.clearSession();

        log.info("ReLoading Config: {}", sessionId);
        HmsMirrorConfig config = configService.loadConfig(sessionId);
        // Remove the old session
        executeSessionService.getSessions().remove(sessionId);
        // Create a new session
        ExecuteSession session = executeSessionService.createSession(sessionId, config);
        executeSessionService.setCurrentSession(session);

        // Set to null, so it will reset.
        session.setSessionId(null);

        configService.validate(session, null);
//        executeSessionService.transitionLoadedSessionToActive(maxThreads);

        // Set it as the current session.
        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);
        model.addAttribute(ACTION, "view");
        model.addAttribute(READ_ONLY, Boolean.FALSE);

        ModelUtils.allEnumsForMap(session.getConfig().getDataStrategy(), model.asMap());

        return "config/view";
    }

    @RequestMapping(value = "/doClone", method = RequestMethod.POST)
    public String doClone(Model model,
                          @RequestParam(value = SESSION_ID_CLONE, required = true) String sessionId,
                          @RequestParam(value = DATA_STRATEGY_CLONE, required = true) String dataStrategy,
                          @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException {
        // Don't reload if running.
        executeSessionService.clearSession();

        log.info("Clone Config: {} with Data Strategy: {}", sessionId, dataStrategy);
        HmsMirrorConfig config = configService.loadConfig(sessionId);

        HmsMirrorConfig newConfig = configService.createForDataStrategy(DataStrategyEnum.valueOf(dataStrategy));

        configService.overlayConfig(newConfig, config);

        // Remove the old session
        executeSessionService.getSessions().remove(sessionId);

        // Create a new session
        ExecuteSession session = executeSessionService.createSession(sessionId, newConfig);
        executeSessionService.setCurrentSession(session);

        // Set to null, so it will reset.
        session.setSessionId(null);

//        executeSessionService.transitionLoadedSessionToActive(maxThreads);

        // Set it as the current session.
        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);
        model.addAttribute(ACTION, "view");
        model.addAttribute(READ_ONLY, Boolean.FALSE);

        ModelUtils.allEnumsForMap(session.getConfig().getDataStrategy(), model.asMap());
        return "config/view";
    }


    @RequestMapping(value = "/view", method = RequestMethod.GET)
    public String view(Model model,
                       @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException {
//        model.addAttribute(ACTION, "view");
        model.addAttribute(READ_ONLY, Boolean.TRUE);
        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);
        return "config/view";
    }

    @RequestMapping(value = "/doEncryptPasswords", method = RequestMethod.GET)
    public String doEncryptPasswords(Model model) throws SessionException, EncryptionException {
        executeSessionService.clearActiveSession();

        // Work with the current session config.  All the values should've been saved already to allow this.
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        if (!config.isEncryptedPasswords() && nonNull(config.getPasswordKey()) && !config.getPasswordKey().isEmpty()) {
            String passwordKey = config.getPasswordKey();
            String lhs2 = null;
            String lms = null;
            String rhs2 = null;
            String rms = null;
            if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getHiveServer2())) {
                lhs2 = passwordService.encryptPassword(passwordKey, config.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getHiveServer2())) {
                rhs2 = passwordService.encryptPassword(passwordKey, config.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
                lms = passwordService.encryptPassword(passwordKey, config.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getMetastoreDirect())) {
                rms = passwordService.encryptPassword(passwordKey, config.getCluster(Environment.RIGHT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
            }
            config.setEncryptedPasswords(Boolean.TRUE);
            if (nonNull(lhs2)) {
                config.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().setProperty("password", lhs2);
            }
            if (nonNull(rhs2)) {
                config.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().setProperty("password", rhs2);
            }
            if (nonNull(lms)) {
                config.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().setProperty("password", lms);
            }
            if (nonNull(rms)) {
                config.getCluster(Environment.RIGHT).getMetastoreDirect().getConnectionProperties().setProperty("password", rms);
            }
        } else {
            throw new SessionException("Inconsistent state: Encrypted, Password Key, etc. . Can't encrypt.");
        }
        return "redirect:/config/view";
    }

    @RequestMapping(value = "/doDecryptPasswords", method = RequestMethod.GET)
    public String doDecryptPasswords(Model model) throws SessionException, EncryptionException {
        executeSessionService.clearActiveSession();

        // Work with the current session config.  All the values should've been saved already to allow this.
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        if (config.isEncryptedPasswords() && nonNull(config.getPasswordKey()) && !config.getPasswordKey().isEmpty()) {
            String passwordKey = config.getPasswordKey();
            String lhs2 = null;
            String lms = null;
            String rhs2 = null;
            String rms = null;
            if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getHiveServer2())) {
                lhs2 = passwordService.decryptPassword(passwordKey, config.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getHiveServer2())) {
                rhs2 = passwordService.decryptPassword(passwordKey, config.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
                lms = passwordService.decryptPassword(passwordKey, config.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getMetastoreDirect())) {
                rms = passwordService.decryptPassword(passwordKey, config.getCluster(Environment.RIGHT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
            }
            config.setEncryptedPasswords(Boolean.FALSE);
            if (nonNull(lhs2)) {
                config.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().setProperty("password", lhs2);
            }
            if (nonNull(rhs2)) {
                config.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().setProperty("password", rhs2);
            }
            if (nonNull(lms)) {
                config.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().setProperty("password", lms);
            }
            if (nonNull(rms)) {
                config.getCluster(Environment.RIGHT).getMetastoreDirect().getConnectionProperties().setProperty("password", rms);
            }
        } else {
            throw new SessionException("Inconsistent state. Encrypted, PasswordKey, etc. . Can't decrypt.");
        }

        return "redirect:/config/view";
    }

}