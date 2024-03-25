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
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ExportCircularResolveService;
import com.cloudera.utils.hms.mirror.service.TableService;
import com.cloudera.utils.hms.mirror.service.TranslatorService;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

import static com.cloudera.utils.hms.mirror.MessageCode.EXPORT_IMPORT_SYNC;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.TRANSLATED_TO_EXTERNAL;

@Component
@Slf4j
public class ExportImportDataStrategy extends DataStrategyBase implements DataStrategy {

    @Getter
    private ExportCircularResolveService exportCircularResolveService;
    @Getter
    private TranslatorService translatorService;
    @Getter
    private ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy;
    @Getter
    private TableService tableService;

    public ExportImportDataStrategy(ConfigService configService) {
        this.configService = configService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        log.debug("Table: " + tableMirror.getName() + " buildout EXPORT_IMPORT Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = tableMirror.getEnvironmentTable(Environment.LEFT);
        ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        if (TableUtils.isACID(let) &&
                !getConfigService().getConfig().getCluster(Environment.LEFT).isLegacyHive()
                        == getConfigService().getConfig().getCluster(Environment.RIGHT).isLegacyHive()) {
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
        if (getConfigService().getConfig().convertManaged())
            copySpec.setUpgrade(Boolean.TRUE);
        if (!getConfigService().getConfig().isReadOnly() || !getConfigService().getConfig().isSync()) {
            copySpec.setTakeOwnership(Boolean.TRUE);
        }
        if (getConfigService().getConfig().isReadOnly()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }
        if (getConfigService().getConfig().isNoPurge()) {
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
            rtn = tableService.buildTableSchema(copySpec);
        return rtn;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        log.debug("Database: " + tableMirror.getName() + " buildout EXPORT_IMPORT SQL");

        String database = null;
        database = getConfigService().getResolvedDB(tableMirror.getParent().getName());

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);
        try {
            // LEFT Export to directory
            String useLeftDb = MessageFormat.format(MirrorConf.USE, tableMirror.getParent().getName());
            let.addSql(TableUtils.USE_DESC, useLeftDb);
            String exportLoc = null;

            if (getConfigService().getConfig().getTransfer().getIntermediateStorage() != null) {
                String isLoc = getConfigService().getConfig().getTransfer().getIntermediateStorage();
                // Deal with extra '/'
                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                exportLoc = isLoc + "/" +
                        getConfigService().getConfig().getTransfer().getRemoteWorkingDirectory() + "/" +
                        getConfigService().getConfig().getRunMarker() + "/" +
                        tableMirror.getParent().getName() + "/" +
                        tableMirror.getName();
            } else if (getConfigService().getConfig().getTransfer().getCommonStorage() != null) {
                String isLoc = getConfigService().getConfig().getTransfer().getCommonStorage();
                // Deal with extra '/'
                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                exportLoc = isLoc + "/" + getConfigService().getConfig().getTransfer().getRemoteWorkingDirectory() + "/" +
                        getConfigService().getConfig().getRunMarker() + "/" +
                        tableMirror.getParent().getName() + "/" +
                        tableMirror.getName();
            } else {
                exportLoc = getConfigService().getConfig().getTransfer().getExportBaseDirPrefix()
                        + tableMirror.getParent().getName() + "/" + let.getName();
            }
            String origTableName = let.getName();
            if (getTableService().isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
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
            if (!getTableService().isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
                String useRightDb = MessageFormat.format(MirrorConf.USE, database);
                ret.addSql(TableUtils.USE_DESC, useRightDb);
            }

            String importLoc = null;
            if (getConfigService().getConfig().getTransfer().getIntermediateStorage() != null
                    || getConfigService().getConfig().getTransfer().getCommonStorage() != null) {
                importLoc = exportLoc;
            } else {
                importLoc = getConfigService().getConfig().getCluster(Environment.LEFT).getHcfsNamespace() + exportLoc;
            }

            String sourceLocation = TableUtils.getLocation(let.getName(), let.getDefinition());
            String targetLocation = getTranslatorService().translateTableLocation(tableMirror, sourceLocation, 1, null);
            String importSql;
            if (TableUtils.isACID(let)) {
                if (!getConfigService().getConfig().getMigrateACID().isDowngrade()) {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_TABLE, let.getName(), importLoc);
                } else {
                    if (getConfigService().getConfig().getMigrateACID().isDowngradeInPlace()) {
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, origTableName, importLoc);
                    } else {
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, let.getName(), importLoc);
                    }
                }
            } else {
                if (getConfigService().getConfig().isResetToDefaultLocation()) {
                    if (getConfigService().getConfig().getTransfer().getWarehouse().getExternalDirectory() != null) {
                        // Build default location, because in some cases when location isn't specified, it will use the "FROM"
                        // location in the IMPORT statement.
                        targetLocation = getConfigService().getConfig().getCluster(Environment.RIGHT).getHcfsNamespace()
                                + getConfigService().getConfig().getTransfer().getWarehouse().getExternalDirectory() +
                                "/" + getConfigService().getResolvedDB(tableMirror.getParent().getName()) + ".db/"
                                + tableMirror.getName();
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION, let.getName(), importLoc, targetLocation);
                    } else {
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, let.getName(), importLoc);
                    }
                } else {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION, let.getName(), importLoc, targetLocation);
                }
            }

            if (ret.isExists()) {
                if (getConfigService().getConfig().isSync()) {
                    // Need to Drop table first.
                    String dropExistingTable = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
                    if (tableService.isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
                        let.addSql(MirrorConf.DROP_TABLE_DESC, dropExistingTable);
                        let.addIssue(EXPORT_IMPORT_SYNC.getDesc());
                    } else {
                        ret.addSql(MirrorConf.DROP_TABLE_DESC, dropExistingTable);
                        ret.addIssue(EXPORT_IMPORT_SYNC.getDesc());
                    }
                }
            }
            if (tableService.isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
                let.addSql(TableUtils.IMPORT_TABLE, importSql);
            } else {
                ret.addSql(TableUtils.IMPORT_TABLE, importSql);
                if (!getConfigService().getConfig().getCluster(Environment.RIGHT).isLegacyHive()
                        && getConfigService().getConfig().isTransferOwnership() && let.getOwner() != null) {
                    String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, let.getName(), let.getOwner());
                    ret.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
                }
            }

            if (let.getPartitions().size() > getConfigService().getConfig().getHybrid().getExportImportPartitionLimit() &&
                    getConfigService().getConfig().getHybrid().getExportImportPartitionLimit() > 0) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                        "limit (hybrid->exportImportPartitionLimit) of "
                        + getConfigService().getConfig().getHybrid().getExportImportPartitionLimit() +
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
        Config config = getConfigService().getConfig();
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);
        if (ret.isExists()) {
            if (!getConfigService().getConfig().isSync()) {
                let.addIssue(MessageCode.SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                return Boolean.FALSE;
            }
        }

        if (tableService.isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
            rtn = getExportImportAcidDowngradeInPlaceDataStrategy().execute(tableMirror);//doEXPORTIMPORTACIDInplaceDowngrade();
        } else {
            if (TableUtils.isACID(let)) {
                if (getConfigService().getConfig().getCluster(Environment.LEFT).isLegacyHive() != config.getCluster(Environment.RIGHT).isLegacyHive()) {
                    rtn = Boolean.FALSE;
                    tableMirror.addIssue(Environment.LEFT, "ACID table EXPORTs are NOT compatible for IMPORT to clusters on a different major version of Hive.");
                } else {
                    rtn = buildOutSql(tableMirror); //tableMirror.buildoutEXPORT_IMPORTSql(config, dbMirror);
                }

            } else {
                rtn = buildOutSql(tableMirror); //tableMirror.buildoutEXPORT_IMPORTSql(config, dbMirror);

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

    @Autowired
    public void setTranslatorService(TranslatorService translatorService) {
        this.translatorService = translatorService;
    }
}
