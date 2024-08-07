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

package com.cloudera.utils.hms.mirror.web.controller.api.v1.config;

import com.cloudera.utils.hms.mirror.domain.Translator;
import com.cloudera.utils.hms.mirror.domain.support.TableType;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.DatabaseService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.TranslatorService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@CrossOrigin
@RestController
@Slf4j
@RequestMapping(path = "/api/v1/translator")
public class TranslatorController {

    private DatabaseService databaseService;
    private ExecuteSessionService executeSessionService;
    private TranslatorService translatorService;

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

    @Operation(summary = "Get Translator Details")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Translator Details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Translator.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/")
    public Translator getTransfer() {
        log.debug("Getting Translator Details");
        return executeSessionService.getSession().getConfig().getTranslator();
    }

    // Translator
    @Operation(summary = "Set Translator Details")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Translator Details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Translator.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/")
    public Translator setTransfer(
            @RequestParam(value = "forceExternalLocation", required = false) Boolean forceExternalLocation ) throws SessionException {

        // Don't reload if running.
        executeSessionService.clearActiveSession();

        if (forceExternalLocation != null) {
            log.info("Setting Translator 'forceExternalLocation' to: {}", forceExternalLocation);
            executeSessionService.getSession().getConfig().getTranslator().setForceExternalLocation(forceExternalLocation);
        }
        return executeSessionService.getSession().getConfig().getTranslator();
    }

    @Operation(summary = "Add GLM")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "GLM Added",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema())}),
            @ApiResponse(responseCode = "400", description = "Invalid environment supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Cluster not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/globalLocationMap")
    public void addGlobalLocationMap(@RequestParam(name = "source", required = true) String source,
                                     @RequestParam(name = "tableType", required = true) TableType tableType,
                                     @RequestParam(name = "target", required = true) String target) throws SessionException {
        log.info("Adding global location map for source: {} and target: {}", source, target);
        // Don't reload if running.
        executeSessionService.clearActiveSession();

        translatorService.addGlobalLocationMap(tableType, source, target);
    }

    @Operation(summary = "Remove GLM")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "GLM Added",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = String.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid environment supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Cluster not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.DELETE, value = "/globalLocationMap")
    public void removeGlobalLocationMap(@RequestParam(name = "source", required = true) String source,
                                          @RequestParam(name = "tableType", required = true) TableType tableType) throws SessionException {
        // Don't reload if running.
        executeSessionService.clearActiveSession();

        log.info("Removing global location map for source: {}", source);
        translatorService.removeGlobalLocationMap(source, tableType);
    }

    // Get Global Location Map
    @Operation(summary = "Get GLM's")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "GLM's retrieved",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Map.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid environment supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Cluster not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/globalLocationMap/list")
    public Map<String, Map<TableType, String>> getGlobalLocationMaps() {
        log.info("Getting global location maps");
        return translatorService.getGlobalLocationMap();
    }

    // Get Global Location Map
    @Operation(summary = "Build GLM from Warehouse Plan")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "GLM Built",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Map.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid environment supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Cluster not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/globalLocationMap/build")
    public Map<String, Map<TableType, String>> buildGLMFromPlans(@RequestParam(name = "dryrun", required = false) Boolean dryrun,
                                                 @RequestParam(name = "buildSources", required = false) Boolean buildSources,
                                                 @RequestParam(name = "partitionLevelMisMatch", required = false) Boolean partitionLevelMisMatch,
                                                 @RequestParam(name = "consolidationLevel", required = false) Integer consolidationLevel)
            throws MismatchException, SessionException, RequiredConfigurationException, EncryptionException {
        log.info("Building global location maps");
        boolean lclDryrun = dryrun != null ? dryrun : true;
        if (!dryrun) {
            executeSessionService.clearActiveSession();
        }
        boolean lclBuildSources = buildSources != null ? buildSources : false;
        int lclConsolidationLevel = consolidationLevel != null ? consolidationLevel : 1;
        boolean lclPartitionLevelMismatch = partitionLevelMisMatch != null && partitionLevelMisMatch;

        if (lclBuildSources) {
            databaseService.buildDatabaseSources(lclConsolidationLevel, false);
        }
        return translatorService.buildGlobalLocationMapFromWarehousePlansAndSources(lclDryrun, lclConsolidationLevel);
    }


}
