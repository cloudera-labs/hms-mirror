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

import com.cloudera.utils.hms.mirror.CopySpec;
import com.cloudera.utils.hms.mirror.CreateStrategy;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.HmsMirrorConfigUtil;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.TableService;
import com.cloudera.utils.hms.mirror.service.TranslatorService;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Slf4j
@Getter
public class SchemaOnlyDataStrategy extends DataStrategyBase implements DataStrategy {

    private ConfigService configService;
//    TranslatorService translatorService;
    private TableService tableService;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    public SchemaOnlyDataStrategy(ExecuteSessionService executeSessionService, TranslatorService translatorService) {
        this.executeSessionService = executeSessionService;
        this.translatorService = translatorService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) throws RequiredConfigurationException {
        Boolean rtn = Boolean.FALSE;
        log.debug("Table: {} buildout SCHEMA_ONLY Definition", tableMirror.getName());
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT, tableMirror);
        ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        copySpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);
        // Swap out the namespace of the LEFT with the RIGHT.
        copySpec.setReplaceLocation(Boolean.TRUE);
        if (hmsMirrorConfig.convertManaged())
            copySpec.setUpgrade(Boolean.TRUE);
        if (!hmsMirrorConfig.isReadOnly() || !hmsMirrorConfig.isSync()) {
            if ((TableUtils.isExternal(let) && hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) ||
                    // Don't add purge for non-legacy environments...
                    // https://github.com/cloudera-labs/hms-mirror/issues/5
                    (TableUtils.isExternal(let) && !hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive())) {
                copySpec.setTakeOwnership(Boolean.FALSE);
            } else {
                copySpec.setTakeOwnership(Boolean.TRUE);
            }
        } else if (copySpec.isUpgrade()) {
            ret.addIssue("Ownership (PURGE Option) not set because of either: `sync` or `ro|read-only` was specified in the config.");
        }
        if (hmsMirrorConfig.isReadOnly()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }
        if (hmsMirrorConfig.isNoPurge()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }

        if (hmsMirrorConfig.isSync()) {
            // We assume that the 'definitions' are only there if the
            //     table exists.
            if (!let.isExists() && ret.isExists()) {
                // If left is empty and right is not, DROP RIGHT.
                ret.addIssue("Schema doesn't exist in 'source'.  Will be DROPPED.");
                ret.setCreateStrategy(CreateStrategy.DROP);
            } else if (let.isExists() && !ret.isExists()) {
                // If left is defined and right is not, CREATE RIGHT.
                ret.addIssue("Schema missing, will be CREATED");
                ret.setCreateStrategy(CreateStrategy.CREATE);
            } else if (let.isExists() && ret.isExists()) {
                // If left and right, check schema change and replace if necessary.
                // Compare Schemas.
                if (tableMirror.schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                    if (let.getPartitioned() && hmsMirrorConfig.loadMetadataDetails()) {
                        ret.setCreateStrategy(CreateStrategy.AMEND_PARTS);
                        ret.addIssue(SCHEMA_EXISTS_SYNC_PARTS.getDesc());
                    } else {
                        ret.addIssue(SCHEMA_EXISTS_NO_ACTION.getDesc());
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                        log.error(TABLE_ISSUE.getDesc(), tableMirror.getParent().getName(), tableMirror.getName(),
                                SCHEMA_EXISTS_NO_ACTION.getDesc());
                    }
                } else {
                    if (TableUtils.isExternalPurge(ret)) {
                        ret.addIssue("Schema exists AND DOESN'T match.  But the 'RIGHT' table is has a PURGE option set. " +
                                "We can NOT safely replace the table without compromising the data. No action will be taken.");
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                        return Boolean.FALSE;
                    } else {
                        ret.addIssue("Schema exists AND DOESN'T match.  It will be REPLACED (DROPPED and RECREATED).");
                        ret.setCreateStrategy(CreateStrategy.REPLACE);
                    }
                }
            }
            copySpec.setTakeOwnership(Boolean.FALSE);
        } else {
            if (ret.isExists()) {
                if (TableUtils.isView(ret)) {
                    ret.addIssue("View exists already.  Will REPLACE.");
                    ret.setCreateStrategy(CreateStrategy.REPLACE);
                } else {
                    if (hmsMirrorConfig.getCluster(Environment.RIGHT).isCreateIfNotExists()) {
                        ret.addIssue("Schema exists already.  But you've specified 'createIfNotExist', which will attempt to create " +
                                "(possibly fail, softly) and continue with the remainder sql statements for the table/partitions.");
                        ret.setCreateStrategy(CreateStrategy.CREATE);
                    } else {
                        // Already exists, no action.
                        ret.addIssue("Schema exists already, no action. If you wish to rebuild the schema, " +
                                "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                        return Boolean.FALSE;
                    }
                }
            } else {
                ret.addIssue("Schema will be created");
                ret.setCreateStrategy(CreateStrategy.CREATE);
            }
        }

        // For ACID tables, we need to remove the location.
        // Hive 3 doesn't allow this to be set via SQL Create.
        if (TableUtils.isACID(let)) {
            copySpec.setStripLocation(Boolean.TRUE);
        }

        // Rebuild Target from Source.
        if (!TableUtils.isACID(let)
                || (TableUtils.isACID(let)
                && hmsMirrorConfig.getMigrateACID().isOn())) {
            rtn = buildTableSchema(copySpec);
        } else {
            let.addIssue(TableUtils.ACID_NOT_ON);
            ret.setCreateStrategy(CreateStrategy.NOTHING);
            rtn = Boolean.FALSE;
        }

        // If not legacy, remove location from ACID tables.
        if (rtn && !hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive() &&
                TableUtils.isACID(let)) {
            TableUtils.stripLocation(let.getName(), let.getDefinition());
        }
        return rtn;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.FALSE;
        log.debug("Table: {} buildout SCHEMA_ONLY SQL", tableMirror.getName());
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        //ret.getSql().clear();

        database = HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config);
        useDb = MessageFormat.format(MirrorConf.USE, database);

        switch (ret.getCreateStrategy()) {
            case NOTHING:
            case LEAVE:
                // Do Nothing.
                break;
            case DROP:
                ret.addSql(TableUtils.USE_DESC, useDb);
                String dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);
                break;
            case REPLACE:
                ret.addSql(TableUtils.USE_DESC, useDb);
                String dropStmt2 = null;
                if (TableUtils.isView(ret)) {
                    dropStmt2 = MessageFormat.format(MirrorConf.DROP_VIEW, ret.getName());
                } else {
                    dropStmt2 = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                }
                ret.addSql(TableUtils.DROP_DESC, dropStmt2);
                String createStmt = getTableService().getCreateStatement(tableMirror, Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt);
                break;
            case CREATE:
                ret.addSql(TableUtils.USE_DESC, useDb);
                String createStmt2 = getTableService().getCreateStatement(tableMirror, Environment.RIGHT);//tableMirror.getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                if (!config.getCluster(Environment.RIGHT).isLegacyHive()
                        && config.getOwnershipTransfer().isTable() && let.getOwner() != null) {
                    String ownerSql = MessageFormat.format(MirrorConf.SET_TABLE_OWNER, ret.getName(), let.getOwner());
                    ret.addSql(MirrorConf.SET_TABLE_OWNER_DESC, ownerSql);
                }
                break;
            case AMEND_PARTS:
                ret.addSql(TableUtils.USE_DESC, useDb);
                break;
        }

        // If partitioned, !ACID, repair
        if (let.getPartitioned() && !TableUtils.isACID(let) &&
                (ret.getCreateStrategy() == CreateStrategy.REPLACE || ret.getCreateStrategy() == CreateStrategy.CREATE
                        || ret.getCreateStrategy() == CreateStrategy.AMEND_PARTS)) {
            if (config.loadMetadataDetails()) {
                // TODO: Write out the SQL to build the partitions.  NOTE: We need to get the partition locations and modify them
                //       to the new namespace.
                String tableParts = getTranslatorService().buildPartitionAddStatement(ret);
                // This will be empty when there's no data and we need to handle that.
                if (!isBlank(tableParts)) {
                    String addPartSql = MessageFormat.format(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION, ret.getName(), tableParts);
                    ret.addSql(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION_DESC, addPartSql);
                }
            } else if (config.getCluster(Environment.RIGHT).getPartitionDiscovery().isInitMSCK()) {
                String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, ret.getName());
                // Add the MSCK repair to both initial and cleanup.
                ret.addSql(TableUtils.REPAIR_DESC, msckStmt);
                if (config.getTransfer().getStorageMigration().isDistcp()) {
                    ret.addCleanUpSql(TableUtils.REPAIR_DESC, msckStmt);
                }
            }
        }

        rtn = Boolean.TRUE;
        return rtn;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        try {
            rtn = this.buildOutDefinition(tableMirror);
        } catch (RequiredConfigurationException e) {
            let.addIssue("Failed to build out definition: " + e.getMessage());
            rtn = Boolean.FALSE;
        }

        if (rtn) {
            rtn = AVROCheck(tableMirror);
        }
        if (rtn) {
            try {
                rtn = this.buildOutSql(tableMirror);
            } catch (MissingDataPointException e) {
                let.addIssue("Failed to build out SQL: " + e.getMessage());
                rtn = Boolean.FALSE;
            }
        }
        if (rtn) {
            rtn = getTableService().runTableSql(tableMirror, Environment.RIGHT);
        }
        return rtn;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }

}
