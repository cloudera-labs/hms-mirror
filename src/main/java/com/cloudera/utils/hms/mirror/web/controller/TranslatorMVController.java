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
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.TableType;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;

import static com.cloudera.utils.hms.mirror.web.controller.ControllerReferences.*;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Controller
@RequestMapping(path = "/translator")
@Slf4j
public class TranslatorMVController {

    private DatabaseService databaseService;
    private ExecuteSessionService executeSessionService;
    private TranslatorService translatorService;
    private ConfigService configService;
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
    public void setTranslatorService(TranslatorService translatorService) {
        this.translatorService = translatorService;
    }

    @Autowired
    public void setUiModelService(UIModelService uiModelService) {
        this.uiModelService = uiModelService;
    }

    @RequestMapping(value = "/globalLocationMap/add", method = RequestMethod.POST)
    public String addGlobalLocationMap(Model model,
                                       @RequestParam(name = TABLE_TYPE, required = true) TableType tableType,
                                       @RequestParam(name = SOURCE, required = true) String source,
                                       @RequestParam(name = TARGET, required = true) String target) throws SessionException, RequiredConfigurationException {
        log.info("Adding global location map for source: {} and target: {}", source, target);
        // Don't reload if running.
        executeSessionService.closeSession();

        if (isBlank(source) || isBlank(target)) {
            throw new RequiredConfigurationException("Source and Target are required (and can't be blank) when adding a global location map.");
        }
        translatorService.addGlobalLocationMap(tableType, source, target);
        uiModelService.sessionToModel(model, 1, Boolean.FALSE);

        return "redirect:/config/view";
    }

    @RequestMapping(value = "/globalLocationMap/delete", method = RequestMethod.POST)
    public String removeGlobalLocationMap(Model model,
                                          @RequestParam String source,
                                          @RequestParam TableType tableType) throws SessionException {
        // Don't reload if running.
        executeSessionService.closeSession();

        log.info("Removing global location map for source: {}", source);
        translatorService.removeGlobalLocationMap(source, tableType);

        uiModelService.sessionToModel(model, 1, Boolean.FALSE);

        return "redirect:/config/view";
    }

    @RequestMapping(method = RequestMethod.POST, value = "/globalLocationMap/build")
    public String buildGLMFromPlans(Model model,
                                    @RequestParam(name = GLM_DRYRUN, required = false) Boolean dryrun,
//                                    @RequestParam(name = BUILD_SOURCES, required = false) Boolean buildSources,
                                    @RequestParam(name = PARTITION_LEVEL_MISMATCH, required = false) Boolean partitionLevelMisMatch,
                                    @RequestParam(name = CONSOLIDATION_LEVEL, required = false) Integer consolidationLevel,
                                    @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads)
            throws MismatchException, SessionException, RequiredConfigurationException, EncryptionException {
        log.info("Building global location maps");
        boolean lclDryrun = dryrun != null ? dryrun : false;

        // Reset Connections and reload most current config.
        executeSessionService.closeSession();
        ExecuteSession session = executeSessionService.getSession();
        configService.validateForConnections(session);

        if (executeSessionService.startSession(maxThreads)) {

            session = executeSessionService.getSession();
            HmsMirrorConfig config = session.getConfig();

            int lclConsolidationLevel = consolidationLevel != null ? consolidationLevel : 1;
            boolean lclPartitionLevelMismatch = partitionLevelMisMatch != null && partitionLevelMisMatch;

            // Don't run if we don't need to.
            if (!config.loadMetadataDetails()) {
                uiModelService.sessionToModel(model, 1, Boolean.FALSE);
                model.addAttribute(TYPE, "Metadata Details");
                model.addAttribute(MESSAGE, "Partition Metadata and GLM's aren't required for this configuration.");
                return "error";
            }

            databaseService.buildDatabaseSources(lclConsolidationLevel, lclPartitionLevelMismatch);
            model.addAttribute(SOURCES, config.getTranslator().getWarehouseMapBuilder().getSources());

            Map<String, Map<TableType, String>> globalLocationMap = null;
            try {
                globalLocationMap = translatorService.buildGlobalLocationMapFromWarehousePlansAndSources(lclDryrun, lclConsolidationLevel);
            } catch (MismatchException e) {
                uiModelService.sessionToModel(model, 1, Boolean.FALSE);
                model.addAttribute(TYPE, "Mismatch");
                model.addAttribute(MESSAGE, e.getMessage());
                return "error";
            }

            if (lclDryrun) {
                HmsMirrorConfig lclConfig = new HmsMirrorConfig();
                lclConfig.getTranslator().setAutoGlobalLocationMap(globalLocationMap);
                lclConfig.getTranslator().setWarehouseMapBuilder(config.getTranslator().getWarehouseMapBuilder());
                model.addAttribute(CONFIG, lclConfig);
                return "translator/globalLocationMap/view";
            } else {
                configService.validate(executeSessionService.getSession(), null);
                return "redirect:/config/view";
            }
        } else {
            uiModelService.sessionToModel(model, 1, Boolean.FALSE);
            model.addAttribute(TYPE, "Connections");
            model.addAttribute(MESSAGE, "Issue validating connections.  Review Messages and try again.");
            return "error";
        }
    }

}
