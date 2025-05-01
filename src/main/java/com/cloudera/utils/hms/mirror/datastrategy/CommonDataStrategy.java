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
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.HmsMirrorConfigUtil;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Slf4j
@Getter
public class CommonDataStrategy extends DataStrategyBase {

    private final ConfigService configService;
    private final TableService tableService;

    public CommonDataStrategy(StatsCalculatorService statsCalculatorService,
                              ExecuteSessionService executeSessionService,
                              TranslatorService translatorService,
                              ConfigService configService,
                              TableService tableService) {
        super(statsCalculatorService, executeSessionService, translatorService);
        this.configService = configService;
        this.tableService = tableService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) throws RequiredConfigurationException {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        log.debug("Table: {} buildout COMMON Definition", tableMirror.getName());
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT, tableMirror);
        ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        copySpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);
        // Can't LINK ACID tables.
        if (let.isExists()) {
            if (TableUtils.isHiveNative(let) && !TableUtils.isACID(let)) {
                // For COMMON, we're assuming the namespace is used by 'both' so we don't change anything..
                copySpec.setReplaceLocation(Boolean.FALSE);
                if (hmsMirrorConfig.convertManaged())
                    copySpec.setUpgrade(Boolean.TRUE);
                // COMMON owns the data unless readonly specified.
                if (!hmsMirrorConfig.isReadOnly())
                    copySpec.setTakeOwnership(Boolean.TRUE);
                if (hmsMirrorConfig.isNoPurge())
                    copySpec.setTakeOwnership(Boolean.FALSE);

                if (hmsMirrorConfig.isSync()) {
                    // We assume that the 'definitions' are only there if the
                    //     table exists.
                    if (let.isExists() && !ret.isExists()) {
                        // If left is defined and right is not, CREATE RIGHT.
                        ret.addIssue("Schema missing, will be CREATED");
                        ret.setCreateStrategy(CreateStrategy.CREATE);
                    } else if (let.isExists() && ret.isExists()) {
                        // If left and right, check schema change and replace if necessary.
                        // Compare Schemas.
                        if (tableMirror.schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                            ret.addIssue(SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                            ret.addSql(SKIPPED.getDesc(), "-- " + SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                            ret.setCreateStrategy(CreateStrategy.LEAVE);
                            String msg = MessageFormat.format(TABLE_ISSUE.getDesc(), tableMirror.getParent().getName(), tableMirror.getName(),
                                    SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                            log.error(msg);
                        } else {
                            if (TableUtils.isExternalPurge(ret)) {
                                ret.addIssue(SCHEMA_EXISTS_NOT_MATCH_WITH_PURGE.getDesc());
                                ret.addSql(SKIPPED.getDesc(), "-- " + SCHEMA_EXISTS_NOT_MATCH_WITH_PURGE.getDesc());
                                ret.setCreateStrategy(CreateStrategy.LEAVE);
                                String msg = MessageFormat.format(TABLE_ISSUE.getDesc(), tableMirror.getParent().getName(), tableMirror.getName(),
                                        SCHEMA_EXISTS_NOT_MATCH_WITH_PURGE.getDesc());
                                log.error(msg);
                                return Boolean.FALSE;
                            } else {
                                ret.addIssue("Schema exists AND DOESN'T match.  It will be REPLACED (DROPPED and RECREATED).");
                                ret.setCreateStrategy(CreateStrategy.REPLACE);
                            }
                        }
                    }
                    // With sync, don't own data.
                    copySpec.setTakeOwnership(Boolean.FALSE);
                } else {
                    if (ret.isExists()) {
                        // Already exists, no action.
                        ret.addIssue(SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                        ret.addSql(SKIPPED.getDesc(), "-- " + SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                        String msg = MessageFormat.format(TABLE_ISSUE.getDesc(), tableMirror.getParent().getName(), tableMirror.getName(),
                                SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                        log.error(msg);
                        return Boolean.FALSE;
                    } else {
                        ret.addIssue("Schema will be created");
                        ret.setCreateStrategy(CreateStrategy.CREATE);
                    }
                }
                // Rebuild Target from Source.
                rtn = buildTableSchema(copySpec);
            } else {
                let.addIssue("Can't use COMMON for ACID tables");
                ret.setCreateStrategy(CreateStrategy.NOTHING);
            }
        } else {
            let.addIssue(SCHEMA_EXISTS_TARGET_MISMATCH.getDesc());
            ret.setCreateStrategy(CreateStrategy.NOTHING);
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        log.debug("Table: {} buildout COMMON SQL", tableMirror.getName());

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
                String dropStmt;
                if (TableUtils.isView(ret))
                    dropStmt = MessageFormat.format(MirrorConf.DROP_VIEW, ret.getName());
                else
                    dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
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
                String createStmt = tableService.getCreateStatement(tableMirror, Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt);
                break;
            case CREATE:
                ret.addSql(TableUtils.USE_DESC, useDb);
                String createStmt2 = tableService.getCreateStatement(tableMirror, Environment.RIGHT);
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
                String tableParts = translatorService.buildPartitionAddStatement(ret);
                // This will be empty when there's no data and we need to handle that.
                if (!isBlank(tableParts)) {
                    String addPartSql = MessageFormat.format(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION, ret.getName(), tableParts);
                    ret.addSql(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION_DESC, addPartSql);
                }
            } else if (config.getCluster(Environment.RIGHT).getPartitionDiscovery().isInitMSCK()) {
                String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, ret.getName());
                if (config.getTransfer().getStorageMigration().isDistcp()) {
                    ret.addCleanUpSql(TableUtils.REPAIR_DESC, msckStmt);
                } else {
                    ret.addSql(TableUtils.REPAIR_DESC, msckStmt);
                }
            }
        }

        rtn = Boolean.TRUE;
        return rtn;
    }

    @Override
    public Boolean build(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        if (TableUtils.isACID(let)) {
            rtn = Boolean.FALSE;
            tableMirror.addIssue(Environment.RIGHT,
                    "Can't transfer SCHEMA reference on COMMON storage for ACID tables.");
        } else {
            try {
                rtn = buildOutDefinition(tableMirror);
            } catch (RequiredConfigurationException e) {
                let.addError("Failed to build out definition: " + e.getMessage());
                rtn = Boolean.FALSE;
            }
        }

        if (rtn) {
            try {
                rtn = buildOutSql(tableMirror);
            } catch (MissingDataPointException e) {
                let.addError("Failed to build out SQL: " + e.getMessage());
                rtn = Boolean.FALSE;
            }
        }

        return rtn;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        return tableService.runTableSql(tableMirror, Environment.RIGHT);
    }

}
