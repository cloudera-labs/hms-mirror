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

package com.cloudera.utils.hms.mirror.datastrategy;

import com.cloudera.utils.hms.mirror.*;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.Warehouse;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.HmsMirrorConfigUtil;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

import static com.cloudera.utils.hms.mirror.MessageCode.EXPORT_IMPORT_SYNC;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.TRANSLATED_TO_EXTERNAL;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.left;

@Component
@Slf4j
@Getter
public class ExportImportDataStrategy extends DataStrategyBase implements DataStrategy {

    private ExportCircularResolveService exportCircularResolveService;
//    private TranslatorService translatorService;
    private ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy;
    private ConfigService configService;
    private DatabaseService databaseService;
    private TableService tableService;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setDatabaseService(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    public ExportImportDataStrategy(ExecuteSessionService executeSessionService, TranslatorService translatorService) {
        this.executeSessionService = executeSessionService;
        this.translatorService = translatorService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) throws RequiredConfigurationException {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        log.debug("Table: {} buildout EXPORT_IMPORT Definition", tableMirror.getName());
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = tableMirror.getEnvironmentTable(Environment.LEFT);
        ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        if (TableUtils.isACID(let) &&
                !hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()
                        == hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
            let.addIssue("Can't process ACID tables with EXPORT_IMPORT between 'legacy' and 'non-legacy' hive environments.  The processes aren't compatible.");
            return Boolean.FALSE;
        }

        if (!TableUtils.isHiveNative(let)) {
            let.addIssue("Can't process ACID tables, VIEWs, or Non Native Hive Tables with this strategy.");
            return Boolean.FALSE;
        }

        copySpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);
        // Swap out the namespace of the LEFT with the RIGHT.
        copySpec.setReplaceLocation(Boolean.TRUE);
        if (hmsMirrorConfig.convertManaged())
            copySpec.setUpgrade(Boolean.TRUE);
        if (!hmsMirrorConfig.isReadOnly() || !hmsMirrorConfig.isSync()) {
            copySpec.setTakeOwnership(Boolean.TRUE);
        }
        if (hmsMirrorConfig.isReadOnly()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }
        if (hmsMirrorConfig.isNoPurge()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }

        if (ret.isExists()) {
            // Already exists, no action.
            ret.addIssue("Schema exists already, no action.  If you wish to rebuild the schema, " +
                    "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
            ret.setCreateStrategy(CreateStrategy.LEAVE);
        } else {
            ret.addIssue("Schema will be created");
            ret.setCreateStrategy(CreateStrategy.CREATE);
            rtn = Boolean.TRUE;
        }
        if (rtn)
            // Build Target from Source.
            rtn = buildTableSchema(copySpec);
        return rtn;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        log.debug("Database: {} buildout EXPORT_IMPORT SQL", tableMirror.getName());

        Warehouse dbWarehouse = databaseService.getWarehousePlan(tableMirror.getParent().getName());

        String database = null;
        database = HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config);

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        String leftNamespace = NamespaceUtils.getNamespace(TableUtils.getLocation(let.getName(), let.getDefinition()));

        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);
        try {
            // LEFT Export to directory
            String useLeftDb = MessageFormat.format(MirrorConf.USE, tableMirror.getParent().getName());
            let.addSql(TableUtils.USE_DESC, useLeftDb);
            String exportLoc = null;

            if (!isBlank(config.getTransfer().getIntermediateStorage())) {
                String isLoc = config.getTransfer().getIntermediateStorage();
                // Deal with extra '/'
                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                exportLoc = isLoc + "/" +
                        config.getTransfer().getRemoteWorkingDirectory() + "/" +
                        config.getRunMarker() + "/" +
                        tableMirror.getParent().getName() + "/" +
                        tableMirror.getName();
            } else if (!isBlank(config.getTransfer().getTargetNamespace())) {
                String isLoc = config.getTransfer().getTargetNamespace();
                // Deal with extra '/'
                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                exportLoc = isLoc + "/" + config.getTransfer().getRemoteWorkingDirectory() + "/" +
                        config.getRunMarker() + "/" +
                        tableMirror.getParent().getName() + "/" +
                        tableMirror.getName();
            } else {
                exportLoc = leftNamespace + config.getTransfer().getExportBaseDirPrefix()
                        + tableMirror.getParent().getName() + "/" + let.getName();
            }
            String origTableName = let.getName();
            if (isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
                // Rename original table.
                // Remove property (if exists) to prevent rename from happening.
                if (TableUtils.hasTblProperty(TRANSLATED_TO_EXTERNAL, let)) {
                    String unSetSql = MessageFormat.format(MirrorConf.REMOVE_TABLE_PROP, origTableName, TRANSLATED_TO_EXTERNAL);
                    let.addSql(MirrorConf.REMOVE_TABLE_PROP_DESC, unSetSql);
                }
                String newTblName = let.getName() + "_archive";
                String renameSql = MessageFormat.format(MirrorConf.RENAME_TABLE, origTableName, newTblName);
                TableUtils.changeTableName(let, newTblName);
                let.addSql(TableUtils.RENAME_TABLE, renameSql);
            }

            String exportSql = MessageFormat.format(MirrorConf.EXPORT_TABLE, let.getName(), exportLoc);
            let.addSql(TableUtils.EXPORT_TABLE, exportSql);

            // RIGHT IMPORT from Directory
            if (!isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
                String useRightDb = MessageFormat.format(MirrorConf.USE, database);
                ret.addSql(TableUtils.USE_DESC, useRightDb);
            }

            String importLoc = exportLoc;
//            if (!isBlank(config.getTransfer().getIntermediateStorage())
//                    || !isBlank(config.getTransfer().getTargetNamespace())) {
//                importLoc = exportLoc;
//            } else {
//                // checked
//                importLoc = leftNamespace + exportLoc;
//            }

            String sourceLocation = TableUtils.getLocation(let.getName(), let.getDefinition());
            String targetLocation = getTranslatorService().translateLocation(tableMirror, sourceLocation, 1, null);
            String importSql;
            if (TableUtils.isACID(let)) {
                if (!config.getMigrateACID().isDowngrade()) {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_TABLE, let.getName(), importLoc);
                } else {
                    if (config.getMigrateACID().isDowngradeInPlace()) {
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, origTableName, importLoc);
                    } else {
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, let.getName(), importLoc);
                    }
                }
            } else {
                if (config.loadMetadataDetails()) {
//                    if (dbWarehouse.getExternalDirectory() != null) {
                        // Build default location, because in some cases when location isn't specified, it will use the "FROM"
                        // location in the IMPORT statement.
                        targetLocation = config.getTargetNamespace()
                                + dbWarehouse.getExternalDirectory() +
                                "/" + HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config) + ".db/"
                                + tableMirror.getName();
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION, let.getName(), importLoc, targetLocation);
//                    } else {
//                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, let.getName(), importLoc);
//                    }
                } else {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION, let.getName(), importLoc, targetLocation);
                }
            }

            if (ret.isExists()) {
                if (config.isSync()) {
                    // Need to Drop table first.
                    String dropExistingTable = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
                    if (isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
                        let.addSql(MirrorConf.DROP_TABLE_DESC, dropExistingTable);
                        let.addIssue(EXPORT_IMPORT_SYNC.getDesc());
                    } else {
                        ret.addSql(MirrorConf.DROP_TABLE_DESC, dropExistingTable);
                        ret.addIssue(EXPORT_IMPORT_SYNC.getDesc());
                    }
                }
            }
            if (isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
                let.addSql(TableUtils.IMPORT_TABLE, importSql);
            } else {
                ret.addSql(TableUtils.IMPORT_TABLE, importSql);
                if (!config.getCluster(Environment.RIGHT).isLegacyHive()
                        && config.isTransferOwnership() && let.getOwner() != null) {
                    String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, let.getName(), let.getOwner());
                    ret.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
                }
            }

            if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit() &&
                    config.getHybrid().getExportImportPartitionLimit() > 0) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                        "limit (hybrid->exportImportPartitionLimit) of "
                        + config.getHybrid().getExportImportPartitionLimit() +
                        ".  This value is used to abort migrations that have a high potential for failure.  " +
                        "The migration will need to be done manually OR try increasing the limit. Review commandline option '-ep'.");
                rtn = Boolean.FALSE;
            } else {
                rtn = Boolean.TRUE;
            }
        } catch (Throwable t) {
            log.error("Error executing EXPORT_IMPORT", t);
            let.addIssue(t.getMessage());
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);
        if (ret.isExists()) {
            if (!hmsMirrorConfig.isSync()) {
                let.addIssue(MessageCode.SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                return Boolean.FALSE;
            }
        }

        if (isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
            rtn = getExportImportAcidDowngradeInPlaceDataStrategy().execute(tableMirror);//doEXPORTIMPORTACIDInplaceDowngrade();
        } else {
            if (TableUtils.isACID(let)) {
                if (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive() != hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
                    rtn = Boolean.FALSE;
                    tableMirror.addIssue(Environment.LEFT, "ACID table EXPORTs are NOT compatible for IMPORT to clusters on a different major version of Hive.");
                } else {
                    try {
                        rtn = buildOutSql(tableMirror); //tableMirror.buildoutEXPORT_IMPORTSql(config, dbMirror);
                    } catch (MissingDataPointException e) {
                        let.addIssue("Failed to build out SQL: " + e.getMessage());
                        rtn = Boolean.FALSE;
                    }
                }

            } else {
                try {
                    rtn = buildOutSql(tableMirror); //tableMirror.buildoutEXPORT_IMPORTSql(config, dbMirror);
                } catch (MissingDataPointException e) {
                    let.addIssue("Failed to build out SQL: " + e.getMessage());
                    rtn = Boolean.FALSE;
                }

                if (rtn)
                    rtn = AVROCheck(tableMirror);
            }
            // If EXPORT_IMPORT, need to run LEFT queries.
            if (rtn) {
                rtn = tableService.runTableSql(tableMirror, Environment.LEFT);
            }

            // Execute the RIGHT sql if config.execute.
            if (rtn) {
                rtn = tableService.runTableSql(tableMirror, Environment.RIGHT);
            }
        }

        return rtn;

    }

    @Autowired
    public void setExportCircularResolveService(ExportCircularResolveService exportCircularResolveService) {
        this.exportCircularResolveService = exportCircularResolveService;
    }

    @Autowired
    public void setExportImportAcidDowngradeInPlaceDataStrategy(ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy) {
        this.exportImportAcidDowngradeInPlaceDataStrategy = exportImportAcidDowngradeInPlaceDataStrategy;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }

}
