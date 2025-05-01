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
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.PersistContainer;
import com.cloudera.utils.hms.mirror.domain.support.SideType;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.mirror.util.ModelUtils;
import com.cloudera.utils.hms.mirror.web.service.WebConfigService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.util.Set;

import static com.cloudera.utils.hms.mirror.web.controller.ReportsMVController.countLines;

@Controller
@RequestMapping(path = "/config")
@Slf4j
public class ConfigMVController implements ControllerReferences {

    private final ConfigService configService;
    private final ExecuteSessionService executeSessionService;
    private final PasswordService passwordService;
    private final ReportService reportService;
    private final WebConfigService webConfigService;
    private final DatabaseService databaseService;
    private final UIModelService uiModelService;

    public ConfigMVController(
            ConfigService configService,
            ExecuteSessionService executeSessionService,
            PasswordService passwordService,
            ReportService reportService,
            WebConfigService webConfigService,
            DatabaseService databaseService,
            UIModelService uiModelService
    ) {
        this.configService = configService;
        this.executeSessionService = executeSessionService;
        this.passwordService = passwordService;
        this.reportService = reportService;
        this.webConfigService = webConfigService;
        this.databaseService = databaseService;
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
        // Get list of available configs
        Set<String> configs = webConfigService.getConfigList();
        model.addAttribute(CONFIG_LIST, configs);
        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);
        return "config/init";
    }

    @RequestMapping(value = "/doCreate", method = RequestMethod.POST)
    public String doCreate(Model model,
                           @RequestParam(value = DATA_STRATEGY, required = true) String dataStrategy,
                           @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads,
                           @Value("${hms-mirror.config.beta}") boolean beta) throws SessionException {
        model.addAttribute(READ_ONLY, Boolean.FALSE);
        // Clear the loaded and active session.
        executeSessionService.closeSession();
        // Create new Session (with blank config)
        HmsMirrorConfig config = configService.createForDataStrategy(DataStrategyEnum.valueOf(dataStrategy));
        // Control Beta Features
        config.setBeta(beta);
        ExecuteSession session = executeSessionService.createSession(NEW_CONFIG, config);
        // Set the Loaded Session
        executeSessionService.setSession(session);
        model.addAttribute(READ_ONLY, Boolean.FALSE);
        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);
        return "config/view";
    }

    @RequestMapping(value = "/edit", method = RequestMethod.GET)
    public String edit(Model model,
                       @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException {
        configService.validate(executeSessionService.getSession(), null);
        model.addAttribute(READ_ONLY, Boolean.FALSE);
        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);
        return "config/view";
    }

    @RequestMapping(value = "/summary", method = RequestMethod.GET)
    public String summary(Model model,
                          @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException {
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        String reportFileString = configService.configToString(config);
        model.addAttribute(SESSION_ID, session.getSessionId());
        model.addAttribute(CONTENT, reportFileString);
        int lines = countLines(reportFileString);
        model.addAttribute(LINES, lines + 3);
        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);
        return "config/summary";
    }

    @RequestMapping(value = "/doSave", method = RequestMethod.POST)
    public String doSave(Model model,
                         @ModelAttribute(CONFIG) HmsMirrorConfig config,
                         @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads,
                         @Value("${hms-mirror.config.beta}") boolean beta) throws SessionException {
        executeSessionService.closeSession();
        // Set beta capabilities. This is driven by the startup of the application.
        config.setBeta(beta);
        if (!executeSessionService.save(config, maxThreads)) {
            return "error";
        }
        ExecuteSession session = executeSessionService.getSession();
        // After a 'save', the session connections statuses should be reset.
        session.resetConnectionStatuses();
        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);
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
        executeSessionService.closeSession();
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
//        model.addAttribute(READ_ONLY, Boolean.TRUE);

        ModelUtils.allEnumsForMap(currentConfig.getDataStrategy(), model.asMap());

        return "config/view";
    }

    @RequestMapping(value = "/doReload", method = RequestMethod.POST)
    public String doReload(Model model,
                           @RequestParam(value = SESSION_ID, required = true) String sessionId,
                           @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads,
                           @Value("${hms-mirror.config.beta}") boolean beta) throws SessionException {
        // Don't reload if running.
        executeSessionService.closeSession();

        log.info("ReLoading Config: {}", sessionId);
        HmsMirrorConfig config = configService.loadConfig(sessionId);
        // Remove the old session
//        executeSessionService.getSessionHistory().remove(sessionId);
        // Create a new session

        // Control Beta Features.
        config.setBeta(beta);

        ExecuteSession session = executeSessionService.createSession(sessionId, config);
        executeSessionService.setSession(session);

        configService.validate(session, null);

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
                          @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads,
                          @Value("${hms-mirror.config.beta}") boolean beta) throws SessionException {
        // Don't reload if running.
        executeSessionService.closeSession();

        log.info("Clone Config: {} with Data Strategy: {}", sessionId, dataStrategy);
        HmsMirrorConfig config = configService.loadConfig(sessionId);

        HmsMirrorConfig newConfig = configService.createForDataStrategy(DataStrategyEnum.valueOf(dataStrategy));

        configService.overlayConfig(newConfig, config);

        // Control Beta Features.
        newConfig.setBeta(beta);

        // Create a new session
        ExecuteSession session = executeSessionService.createSession(sessionId, newConfig);
        executeSessionService.setSession(session);

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
        configService.validate(executeSessionService.getSession(), null);
//        model.addAttribute(READ_ONLY, Boolean.TRUE);
        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);
        return "config/view";
    }

    @RequestMapping(value = "/optimization/overrides/add", method = RequestMethod.POST)
    public String addOverrideProperty(Model model,
                                      @RequestParam(value = PROPERTY, required = true) String property,
                                      @RequestParam(value = VALUE, required = true) String value,
                                      @RequestParam(value = SIDE, required = true) SideType side) throws SessionException {
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        config.getOptimization().getOverrides().addProperty(property, value, side);
        // After a 'save', the session connections statuses should be reset.
        session.resetConnectionStatuses();

        return "redirect:/config/view";
    }

    @RequestMapping(value = "/optimization/overrides/{property}/{side}/delete", method = RequestMethod.GET)
    public String deleteOverride(Model model,
                                      @PathVariable @NotNull String property,
                                 @PathVariable @NotNull SideType side) throws SessionException {
        executeSessionService.closeSession();

        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        try {
            config.getOptimization().getOverrides().getProperties().get(property).remove(side);

            // After a 'save', the session connections statuses should be reset.
            session.resetConnectionStatuses();

        } catch (Exception e) {
            log.error("Error deleting property: {}", e.getMessage());
        }

        return "redirect:/config/view";
    }

}