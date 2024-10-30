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

import com.cloudera.utils.hms.mirror.domain.*;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.web.service.WebConfigService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.isNull;

@CrossOrigin
@RestController
@Slf4j
@RequestMapping(path = "/api/v1/config")
public class ConfigController {

    private org.springframework.core.env.Environment springEnv;

    private ConfigService configService;
    private WebConfigService webConfigService;
    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setSpringEnv(org.springframework.core.env.Environment springEnv) {
        this.springEnv = springEnv;
    }

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setWebConfigService(WebConfigService webConfigService) {
        this.webConfigService = webConfigService;
    }

    @Autowired
    public void setHmsMirrorCfgService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Operation(summary = "Get a list of available configs")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Found list of Configs",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = List.class))})
//            , @ApiResponse(responseCode = "400", description = "Invalid id supplied",
//                    content = @Content)
//            , @ApiResponse(responseCode = "404", description = "Config not found",
//                    content = @Content)
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/list")
    public Set<String> getConfigList() {
        log.info("Getting Config List");
        return webConfigService.getConfigList();
    }

    @Operation(summary = "Get session config")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Current Config",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = HmsMirrorConfig.class))})
//            , @ApiResponse(responseCode = "400", description = "Invalid id supplied",
//                    content = @Content)
//            , @ApiResponse(responseCode = "404", description = "Config not found",
//                    content = @Content)
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/")
    public HmsMirrorConfig getConfig(@RequestParam(name = "sessionId", required = false) String sessionId) {
        log.info("Getting Config: {}", sessionId);
        return executeSessionService.getSession(sessionId).getConfig();
    }

    @Operation(summary = "Get the config by id")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Found the Catalog",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = HmsMirrorConfig.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid id supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Config not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/{id}")
    public HmsMirrorConfig getById(@PathVariable @NotNull String id) {
        log.info("Getting Config by id: {}", id);
        String configPath = springEnv.getProperty("hms-mirror.config.path");
        String configFileName = configPath + File.separator + id;
        return configService.loadConfig(configFileName);
    }

//    private boolean isCurrentSessionRunning() {
//        boolean rtn = Boolean.FALSE;
//        // Need to check that nothing is running currently.
//        ExecuteSession session = executeSessionService.getActiveSession();
//        if (session != null) {
//            RunStatus runStatus = session.getRunStatus();
//            if (runStatus.isRunning()) {
//                rtn = Boolean.TRUE;
//            }
//        }
//        return rtn;
//    }

    @Operation(summary = "Load a config by id")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Config loaded",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = HmsMirrorConfig.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid input",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/load/{id}")
    public HmsMirrorConfig load(@RequestParam(name = "sessionId", required = false) String sessionId,
                                @PathVariable @NotNull String id) {
        log.info("{}: Loading Config by id: {}", sessionId, id);
        HmsMirrorConfig config = configService.loadConfig(id);
        // If they aren't setting the session id, use the id as the session id.
//        String targetSessionId = sessionId != null? sessionId : id;
        // Get the session and set the config.
        ExecuteSession session = executeSessionService.getSession(sessionId);
        if (isNull(session)) {
            session = executeSessionService.createSession(sessionId, config);
        } else {
            session.setConfig(config);
        }
        // Set it as the current session.
        executeSessionService.setSession(session);
        return config;
    }

    @Operation(summary = "Re-Load a config by id (from the filesystem)")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Config loaded",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = HmsMirrorConfig.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid input",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/reload/{id}")
    public HmsMirrorConfig reload(@PathVariable @NotNull String id) throws SessionException {
        // Don't reload if running.
        executeSessionService.closeSession();

        log.info("ReLoading Config: {}", id);
        HmsMirrorConfig config = configService.loadConfig(id);
        // Remove the old session
        executeSessionService.getSessionHistory().remove(id);
        // Create a new session
        ExecuteSession session = executeSessionService.createSession(id, config);

        // Set it as the current session.
        executeSessionService.setSession(session);

        return config;
    }

    @Operation(summary = "Save 'current' config to id")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Config Saved",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Boolean.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid input",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/save/{id}")
    public boolean save(@RequestParam(name = "sessionId", required = false) String sessionId,
                        @PathVariable @NotNull String id,
                        @RequestParam(value = "overwrite", required = false) Boolean overwrite) throws IOException {
        log.info("{}: Save current config to: {}", sessionId, id);
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        //Session(sessionId).getHmsMirrorConfig();
        // Save to the hms-mirror.config.path as 'id'.
        String configPath = springEnv.getProperty("hms-mirror.config.path");
        String configFullFilename = configPath + File.separator + id;
        return configService.saveConfig(config, configFullFilename, overwrite);
    }

    @Operation(summary = "Get the configs clusters")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Found the Clusters",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Map.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid id supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Cluster not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/clusters")
    public Map<Environment, Cluster> getClusters(@RequestParam(name = "sessionId", required = false) String sessionId) {
        log.info("{}: Getting Clusters for current Config", sessionId);
        return executeSessionService.getSession().getConfig().getClusters();
    }

    @Operation(summary = "Get the LEFT|RIGHT cluster config")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Found the Clusters",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Cluster.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid id supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Cluster not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/clusters/{side}")
    public Cluster getCluster(@RequestParam(name = "sessionId", required = false) String sessionId,
                              @PathVariable @NotNull String side) {
        log.info("{}: Getting {} Cluster current Config", sessionId, side);
        String sideStr = side.toUpperCase();
        Environment env = Environment.valueOf(sideStr);
        return executeSessionService.getSession().getConfig().getClusters().get(env);
    }

    @Operation(summary = "Set Config Properties")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Set Properties",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = HmsMirrorConfig.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid id supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Cluster not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/properties")
    public HmsMirrorConfig setConfigProperties(
            @RequestParam(name = "sessionId", required = false) String sessionId,
            @RequestParam(value = "copyAvroSchemaUrls", required = false) Boolean copyAvroSchemaUrls,
            @RequestParam(value = "dataStrategy", required = false) DataStrategyEnum dataStrategy,
            @RequestParam(value = "databaseOnly", required = false) Boolean databaseOnly,
            @RequestParam(value = "databases", required = false) Set<String> databases,
            @RequestParam(value = "dbPrefix", required = false) String dbPrefix,
            @RequestParam(value = "dbRename", required = false) String dbRename,
            @RequestParam(value = "evaluatePartitionLocation", required = false) Boolean evaluatePartitionLocation,
            @RequestParam(value = "flip", required = false) Boolean flip,
            @RequestParam(value = "migratedNonNative", required = false) Boolean migratedNonNative,
            @RequestParam(value = "outputDirectory", required = false) String outputDirectory,
            @RequestParam(value = "readOnly", required = false) Boolean readOnly,
            @RequestParam(value = "noPurge", required = false) Boolean noPurge,
            @RequestParam(value = "replace", required = false) Boolean replace,
            @RequestParam(value = "resetToDefaultLocation", required = false) Boolean resetToDefaultLocation,
            @RequestParam(value = "skipFeatures", required = false) Boolean skipFeatures,
            @RequestParam(value = "skipLegacyTranslation", required = false) Boolean skipLegacyTranslation,
            @RequestParam(value = "sync", required = false) Boolean sync,
            @RequestParam(value = "transferOwnershipDb", required = false) Boolean transferOwnershipDb,
            @RequestParam(value = "transferOwnershipTbl", required = false) Boolean transferOwnershipTbl
    ) throws SessionException {
        // Don't reload if running.
        executeSessionService.closeSession();

        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        if (copyAvroSchemaUrls != null) {
            log.info("{}: Setting Copy Avro Schema Urls to: {}", sessionId, copyAvroSchemaUrls);
            hmsMirrorConfig.setCopyAvroSchemaUrls(copyAvroSchemaUrls);
        }
        if (dataStrategy != null) {
            log.info("{}: Setting Data Strategy to: {}", sessionId, dataStrategy);
            hmsMirrorConfig.setDataStrategy(dataStrategy);
        }
        if (databaseOnly != null) {
            log.info("{}: Setting Database Only to: {}", sessionId, databaseOnly);
            hmsMirrorConfig.setDatabaseOnly(databaseOnly);
        }
        if (databases != null) {
            log.info("{}: Setting Databases to: {}", sessionId, databases);
            hmsMirrorConfig.setDatabases(databases);
        }
        if (dbPrefix != null) {
            log.info("{}: Setting Database Prefix to: {}", sessionId, dbPrefix);
            hmsMirrorConfig.setDbPrefix(dbPrefix);
        }
        if (dbRename != null) {
            log.info("{}: Setting Database Rename to: {}", sessionId, dbRename);
            hmsMirrorConfig.setDbRename(dbRename);
        }
//        if (evaluatePartitionLocation != null) {
//            log.info("{}: Setting Evaluate Partition Location to: {}", sessionId, evaluatePartitionLocation);
//            hmsMirrorConfig.setEvaluatePartitionLocation(evaluatePartitionLocation);
//        }
        if (flip != null) {
            log.info("{}: Setting Flip to: {}", sessionId, flip);
            hmsMirrorConfig.setFlip(flip);
        }
        if (migratedNonNative != null) {
            log.info("{}: Setting Migrated Non Native to: {}", sessionId, migratedNonNative);
            hmsMirrorConfig.setMigrateNonNative(migratedNonNative);
        }
        if (outputDirectory != null) {
            log.info("{}: Setting Output Directory to: {}", sessionId, outputDirectory);
            hmsMirrorConfig.setOutputDirectory(outputDirectory);
        }
        if (readOnly != null) {
            log.info("{}: Setting Read Only to: {}", sessionId, readOnly);
            hmsMirrorConfig.setReadOnly(readOnly);
        }
        if (noPurge != null) {
            log.info("{}: Setting No Purge to: {}", sessionId, noPurge);
            hmsMirrorConfig.setNoPurge(noPurge);
        }
        if (replace != null) {
            log.info("{}: Setting Replace to: {}", sessionId, replace);
            hmsMirrorConfig.setReplace(replace);
        }
//        if (resetToDefaultLocation != null) {
//            log.info("{}: Setting Reset To Default Location to: {}", sessionId, resetToDefaultLocation);
//            hmsMirrorConfig.setResetToDefaultLocation(resetToDefaultLocation);
//        }
        if (skipFeatures != null) {
            log.info("{}: Setting Skip Features to: {}", sessionId, skipFeatures);
            hmsMirrorConfig.setSkipFeatures(skipFeatures);
        }
        if (skipLegacyTranslation != null) {
            log.info("{}: Setting Skip Legacy Translation to: {}", sessionId, skipLegacyTranslation);
            hmsMirrorConfig.setSkipLegacyTranslation(skipLegacyTranslation);
        }
        if (sync != null) {
            log.info("{}: Setting Sync to: {}", sessionId, sync);
            hmsMirrorConfig.setSync(sync);
        }
        if (transferOwnershipDb != null) {
            log.info("{}: Setting Transfer Database Ownership to: {}", sessionId, transferOwnershipDb);
            hmsMirrorConfig.getOwnershipTransfer().setDatabase(transferOwnershipDb);
        }
        if (transferOwnershipTbl != null) {
            log.info("{}: Setting Transfer Table Ownership to: {}", sessionId, transferOwnershipTbl);
            hmsMirrorConfig.getOwnershipTransfer().setTable(transferOwnershipTbl);
        }
        return hmsMirrorConfig;
    }

    @Operation(summary = "Set the Table Filters")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Table Filter set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Filter.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid Database list",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/tableFilter")
    public Filter setTableFilter(@RequestParam(name = "sessionId", required = false) String sessionId,
                                 @RequestParam(value = "tblExcludeRegEx", required = false) String excludeRegEx,
                                 @RequestParam(value = "tblRegEx", required = false) String regEx,
                                 @RequestParam(value = "tblSizeLimit", required = false) String tblSizeLimit,
                                 @RequestParam(value = "tblPartitionLimit", required = false) String tblPartitionLimit) throws SessionException {

        // Don't reload if running.
        executeSessionService.closeSession();

        Filter filter = executeSessionService.getSession().getConfig().getFilter();

        if (excludeRegEx != null) {
            log.info("{}: Setting Table Exclude RegEx to: {}", sessionId, excludeRegEx);
            filter.setTblExcludeRegEx(excludeRegEx);
        }
        if (regEx != null) {
            log.info("{}: Setting Table RegEx to: {}", sessionId, regEx);
            filter.setTblRegEx(regEx);
        }
        if (tblSizeLimit != null) {
            log.info("{}: Setting Table Size Limit to: {}", sessionId, tblSizeLimit);
            filter.setTblSizeLimit(Long.parseLong(tblSizeLimit));
        }
        if (tblPartitionLimit != null) {
            log.info("{}: Setting Table Partition Limit to: {}", sessionId, tblPartitionLimit);
            filter.setTblPartitionLimit(Integer.parseInt(tblPartitionLimit));
        }
        return filter;
    }


    @Operation(summary = "Set the Migrate Options")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Migrate Options set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = HmsMirrorConfig.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid Database list",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/migrate")
    public HmsMirrorConfig setMigrate(@RequestParam(name = "sessionId", required = false) String sessionId,
                                      @RequestParam(value = "acid", required = false) Boolean acid,
                                      @RequestParam(value = "acid-only", required = false) Boolean acidOnly,
                                      @RequestParam(value = "acid-artificialBucketThreshold", required = false) String artificialBucketThreshold,
                                      @RequestParam(value = "acid-partitionLimit", required = false) String partitionLimit,
                                      @RequestParam(value = "acid-downgrade", required = false) Boolean downgrade,
                                      @RequestParam(value = "acid-inplace-downgrade", required = false) Boolean inplace,
                                      @RequestParam(value = "non-native", required = false) Boolean nonNative,
                                      @RequestParam(value = "views", required = false) Boolean views) throws SessionException {

        // Don't reload if running.
        executeSessionService.closeSession();

        MigrateACID migrateACID = executeSessionService.getSession().getConfig().getMigrateACID();

        if (acid != null) {
            log.info("{}: Setting Migrate ACID 'on' to: {}", sessionId, acid);
            migrateACID.setOn(acid);
        }
        if (acidOnly != null) {
            log.info("{}: Setting Migrate ACID 'only' to: {}", sessionId, acidOnly);
            migrateACID.setOnly(acidOnly);
        }
        if (artificialBucketThreshold != null) {
            log.info("{}: Setting Migrate ACID 'artificialBucketThreshold' to: {}", sessionId, artificialBucketThreshold);
            migrateACID.setArtificialBucketThreshold(Integer.parseInt(artificialBucketThreshold));
        }
        if (partitionLimit != null) {
            log.info("{}: Setting Migrate ACID 'partitionLimit' to: {}", sessionId, partitionLimit);
            migrateACID.setPartitionLimit(Integer.parseInt(partitionLimit));
        }
        if (downgrade != null) {
            log.info("{}: Setting Migrate ACID 'downgrade' to: {}", sessionId, downgrade);
            migrateACID.setDowngrade(downgrade);
        }
        if (inplace != null) {
            log.info("{}: Setting Migrate ACID 'inplace' to: {}", sessionId, inplace);
            migrateACID.setInplace(inplace);
        }
        if (nonNative != null) {
            log.info("{}: Setting Migrate ACID 'nonNative' to: {}", sessionId, nonNative);
            executeSessionService.getSession().getConfig().setMigrateNonNative(nonNative);
        }
        if (views != null) {
            log.info("{}: Setting Migrate ACID 'views' to: {}", sessionId, views);
            executeSessionService.getSession().getConfig().getMigrateVIEW().setOn(views);
        }
        return executeSessionService.getSession().getConfig();
    }

    // Transfer
    @Operation(summary = "Set Transfer Details")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Transfer details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = TransferConfig.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/transfer")
    public TransferConfig setTransfer(@RequestParam(name = "sessionId", required = false) String sessionId,
                                      @RequestParam(value = "transferPrefix", required = false) String transferPrefix,
                                      @RequestParam(value = "shadowPrefix", required = false) String shadowPrefix,
                                      @RequestParam(value = "exportBaseDirPrefix", required = false) String exportBaseDirPrefix,
                                      @RequestParam(value = "remoteWorkingDirectory", required = false) String remoteWorkingDirectory,
                                      @RequestParam(value = "intermediateStorage", required = false) String intermediateStorage,
                                      @RequestParam(value = "commonStorage", required = false) String commonStorage
    ) throws SessionException {

        // Don't reload if running.
        executeSessionService.closeSession();

        TransferConfig transferConfig = executeSessionService.getSession().getConfig().getTransfer();

        if (transferPrefix != null) {
            log.info("{}: Setting Transfer 'transferPrefix' to: {}", sessionId, transferPrefix);
            transferConfig.setTransferPrefix(transferPrefix);
        }
        if (shadowPrefix != null) {
            log.info("{}: Setting Transfer 'shadowPrefix' to: {}", sessionId, shadowPrefix);
            transferConfig.setShadowPrefix(shadowPrefix);
        }
        if (exportBaseDirPrefix != null) {
            log.info("{}: Setting Transfer 'exportBaseDirPrefix' to: {}", sessionId, exportBaseDirPrefix);
            transferConfig.setExportBaseDirPrefix(exportBaseDirPrefix);
        }
        if (remoteWorkingDirectory != null) {
            log.info("{}: Setting Transfer 'remoteWorkingDirectory' to: {}", sessionId, remoteWorkingDirectory);
            transferConfig.setRemoteWorkingDirectory(remoteWorkingDirectory);
        }
        if (intermediateStorage != null) {
            log.info("{}: Setting Transfer 'intermediateStorage' to: {}", sessionId, intermediateStorage);
            transferConfig.setIntermediateStorage(intermediateStorage);
        }
        if (commonStorage != null) {
            log.info("{}: Setting Transfer 'commonStorage' to: {}", sessionId, commonStorage);
            transferConfig.setTargetNamespace(commonStorage);
        }
        return transferConfig;
    }

    // Transfer / Warehouse
    @Operation(summary = "Set Warehouse Details")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Warehouse Details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Warehouse.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/transfer/warehouse")
    public Warehouse setTransferWarehouse(@RequestParam(name = "sessionId", required = false) String sessionId,
                                                @RequestParam(value = "managedDirectory", required = false) String managedDirectory,
                                                @RequestParam(value = "externalDirectory", required = false) String externalDirectory
    ) throws SessionException {
        // Don't reload if running.
        executeSessionService.closeSession();

        Warehouse warehouseConfig = executeSessionService.getSession().getConfig().getTransfer().getWarehouse();

        if (managedDirectory != null) {
            log.info("{}: Setting Warehouse 'managedDirectory' to: {}", sessionId, managedDirectory);
            warehouseConfig.setManagedDirectory(managedDirectory);
        }
        if (externalDirectory != null) {
            log.info("{}: Setting Warehouse 'externalDirectory' to: {}", sessionId, externalDirectory);
            warehouseConfig.setExternalDirectory(externalDirectory);
        }
        return warehouseConfig;
    }

    // Transfer / Storage Migration
    @Operation(summary = "Set Storage Migration Details")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Storage Migration Details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = StorageMigration.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/transfer/storageMigration")
    public StorageMigration setStorageMigration(@RequestParam(name = "sessionId", required = false) String sessionId,
                                                @RequestParam(value = "dataMovementStrategy", required = false) DataMovementStrategyEnum dataMovementStrategy,
                                                @RequestParam(value = "dataFlow", required = false) DistcpFlowEnum dataFlow,
                                                @RequestParam(value = "strict", required = false) Boolean strict
    ) throws SessionException {

        // Don't reload if running.
        executeSessionService.closeSession();

        StorageMigration storageMigration = executeSessionService.getSession().getConfig().getTransfer().getStorageMigration();

        if (dataMovementStrategy != null) {
            log.info("{}: Setting Storage Migration 'dataMovementStrategy' to: {}", sessionId, dataMovementStrategy);
            storageMigration.setDataMovementStrategy(dataMovementStrategy);
        }
        if (dataFlow != null) {
            log.info("{}: Setting Storage Migration 'dataFlow' to: {}", sessionId, dataFlow);
            storageMigration.setDataFlow(dataFlow);
        }
        if (strict != null) {
            log.info("{}: Setting Storage Migration 'strict' to: {}", sessionId, strict);
            storageMigration.setStrict(strict);
        }
        return storageMigration;
    }

}
