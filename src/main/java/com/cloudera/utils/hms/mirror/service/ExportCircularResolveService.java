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

import com.cloudera.utils.hms.mirror.Environment;
import com.cloudera.utils.hms.mirror.EnvironmentTable;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.TableMirror;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategyBase;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;

import static com.cloudera.utils.hms.mirror.MessageCode.EXPORT_IMPORT_SYNC;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.TRANSLATED_TO_EXTERNAL;

@Service
@Slf4j
@Getter
public class ExportCircularResolveService extends DataStrategyBase {

    private final HmsMirrorCfgService hmsMirrorCfgService;
    private TableService tableService;
    private TranslatorService translatorService;

    public ExportCircularResolveService(HmsMirrorCfgService hmsMirrorCfgService) {
        this.hmsMirrorCfgService = hmsMirrorCfgService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        return null;
    }

    public Boolean buildOutExportImportSql(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        log.debug("Database: {} buildout EXPORT_IMPORT SQL", tableMirror.getName());

        String database = null;
        database = getHmsMirrorCfgService().getResolvedDB(tableMirror.getParent().getName());

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);
        try {
            // LEFT Export to directory
            String useLeftDb = MessageFormat.format(MirrorConf.USE, tableMirror.getParent().getName());
            let.addSql(TableUtils.USE_DESC, useLeftDb);
            String exportLoc = null;

            if (getHmsMirrorCfgService().getHmsMirrorConfig().getTransfer().getIntermediateStorage() != null) {
                String isLoc = getHmsMirrorCfgService().getHmsMirrorConfig().getTransfer().getIntermediateStorage();
                // Deal with extra '/'
                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                exportLoc = isLoc + "/" +
                        getHmsMirrorCfgService().getHmsMirrorConfig().getTransfer().getRemoteWorkingDirectory() + "/" +
                        getHmsMirrorCfgService().getHmsMirrorConfig().getRunMarker() + "/" +
//                    config.getTransfer().getTransferPrefix() + this.getUnique() + "_" +
                        tableMirror.getParent().getName() + "/" +
                        tableMirror.getName();
            } else if (getHmsMirrorCfgService().getHmsMirrorConfig().getTransfer().getCommonStorage() != null) {
                String isLoc = getHmsMirrorCfgService().getHmsMirrorConfig().getTransfer().getCommonStorage();
                // Deal with extra '/'
                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                exportLoc = isLoc + "/" + getHmsMirrorCfgService().getHmsMirrorConfig().getTransfer().getRemoteWorkingDirectory() + "/" +
                        getHmsMirrorCfgService().getHmsMirrorConfig().getRunMarker() + "/" +
//                    config.getTransfer().getTransferPrefix() + this.getUnique() + "_" +
                        tableMirror.getParent().getName() + "/" +
                        tableMirror.getName();
            } else {
                exportLoc = getHmsMirrorCfgService().getHmsMirrorConfig().getTransfer().getExportBaseDirPrefix()
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
            if (getHmsMirrorCfgService().getHmsMirrorConfig().getTransfer().getIntermediateStorage() != null
                    || getHmsMirrorCfgService().getHmsMirrorConfig().getTransfer().getCommonStorage() != null) {
                importLoc = exportLoc;
            } else {
                importLoc = getHmsMirrorCfgService().getHmsMirrorConfig().getCluster(Environment.LEFT).getHcfsNamespace() + exportLoc;
            }

            String sourceLocation = TableUtils.getLocation(let.getName(), let.getDefinition());
            String targetLocation = getTranslatorService().translateTableLocation(tableMirror, sourceLocation, 1, null);
            String importSql;
            if (TableUtils.isACID(let)) {
                if (!getHmsMirrorCfgService().getHmsMirrorConfig().getMigrateACID().isDowngrade()) {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_TABLE, let.getName(), importLoc);
                } else {
                    if (getHmsMirrorCfgService().getHmsMirrorConfig().getMigrateACID().isDowngradeInPlace()) {
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, origTableName, importLoc);
                    } else {
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, let.getName(), importLoc);
                    }
                }
            } else {
                if (getHmsMirrorCfgService().getHmsMirrorConfig().isResetToDefaultLocation()) {
                    if (getHmsMirrorCfgService().getHmsMirrorConfig().getTransfer().getWarehouse().getExternalDirectory() != null) {
                        // Build default location, because in some cases when location isn't specified, it will use the "FROM"
                        // location in the IMPORT statement.
                        targetLocation = getHmsMirrorCfgService().getHmsMirrorConfig().getCluster(Environment.RIGHT).getHcfsNamespace()
                                + getHmsMirrorCfgService().getHmsMirrorConfig().getTransfer().getWarehouse().getExternalDirectory() +
                                "/" + getHmsMirrorCfgService().getResolvedDB(tableMirror.getParent().getName()) + ".db/"
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
                if (getHmsMirrorCfgService().getHmsMirrorConfig().isSync()) {
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
                if (!getHmsMirrorCfgService().getHmsMirrorConfig().getCluster(Environment.RIGHT).isLegacyHive()
                        && getHmsMirrorCfgService().getHmsMirrorConfig().isTransferOwnership() && let.getOwner() != null) {
                    String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, let.getName(), let.getOwner());
                    ret.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
                }
            }

            if (let.getPartitions().size() > getHmsMirrorCfgService().getHmsMirrorConfig().getHybrid().getExportImportPartitionLimit() &&
                    getHmsMirrorCfgService().getHmsMirrorConfig().getHybrid().getExportImportPartitionLimit() > 0) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                        "limit (hybrid->exportImportPartitionLimit) of "
                        + getHmsMirrorCfgService().getHmsMirrorConfig().getHybrid().getExportImportPartitionLimit() +
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
    public Boolean buildOutSql(TableMirror tableMirror) {
        return null;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        return null;
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
