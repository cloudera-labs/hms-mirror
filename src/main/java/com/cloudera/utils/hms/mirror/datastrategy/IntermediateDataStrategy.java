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
import com.cloudera.utils.hms.mirror.service.HmsMirrorCfgService;
import com.cloudera.utils.hms.mirror.service.TableService;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hms.mirror.SessionVars.SET_TEZ_AS_EXECUTION_ENGINE;
import static com.cloudera.utils.hms.mirror.SessionVars.TEZ_EXECUTION_DESC;

@Component
@Slf4j
@Getter
public class IntermediateDataStrategy extends DataStrategyBase implements DataStrategy {

    private TableService tableService;

    public IntermediateDataStrategy(HmsMirrorCfgService hmsMirrorCfgService) {
        this.hmsMirrorCfgService = hmsMirrorCfgService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

        log.debug("Table: {} buildout Intermediate Definition", tableMirror.getName());
        EnvironmentTable let = null;
        EnvironmentTable ret = null;

        let = getEnvironmentTable(Environment.LEFT, tableMirror);
        ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        CopySpec rightSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);

        if (ret.isExists()) {
            if (!TableUtils.isACID(ret) && hmsMirrorConfig.getCluster(Environment.RIGHT).isCreateIfNotExists() && hmsMirrorConfig.isSync()) {
                ret.addIssue(CINE_WITH_EXIST.getDesc());
                ret.setCreateStrategy(CreateStrategy.CREATE);
            } else if (TableUtils.isACID(ret) && getHmsMirrorCfgService().getHmsMirrorConfig().isSync()) {
                ret.addIssue(SCHEMA_EXISTS_SYNC_ACID.getDesc());
                ret.setCreateStrategy(CreateStrategy.REPLACE);
            } else {
                // Already exists, no action.
                ret.addIssue(SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                ret.setCreateStrategy(CreateStrategy.NOTHING);
                return Boolean.FALSE;
            }
        } else {
            ret.setCreateStrategy(CreateStrategy.CREATE);
        }

        if (!TableUtils.isACID(let) && TableUtils.isManaged(let)) {
            // Managed to EXTERNAL
            rightSpec.setUpgrade(Boolean.TRUE);
            rightSpec.setReplaceLocation(Boolean.TRUE);
        } else if (TableUtils.isACID(let)) {
            // ACID
            if (hmsMirrorConfig.getMigrateACID().isDowngrade()) {
                if (hmsMirrorConfig.getTransfer().getCommonStorage() == null) {
                    if (hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
                        rightSpec.setReplaceLocation(Boolean.TRUE);
                    } else {
                        rightSpec.setStripLocation(Boolean.TRUE);
                    }
                } else {
                    rightSpec.setReplaceLocation(Boolean.TRUE);
                }
                rightSpec.setMakeExternal(Boolean.TRUE);
                // Strip the Transactional Elements
                rightSpec.setMakeNonTransactional(Boolean.TRUE);
                // Set Purge Flag
                rightSpec.setTakeOwnership(Boolean.TRUE);
            } else {
                // Use the system default location when converting.
                rightSpec.setStripLocation(Boolean.TRUE);
            }
        } else {
            // External
            rightSpec.setReplaceLocation(Boolean.TRUE);
        }

        // Build Target from Source.
        rtn = tableService.buildTableSchema(rightSpec);
        //tableMirror.buildTableSchema(rightSpec);

        // Build Transfer Spec.
        CopySpec transferSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.TRANSFER);
        if (hmsMirrorConfig.getTransfer().getCommonStorage() == null) {
            if (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()
                    != hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
                if (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                    transferSpec.setMakeNonTransactional(Boolean.TRUE);
                } else {
                    // Don't support non-legacy to legacy conversions.
                    let.addIssue("Don't support Non-Legacy to Legacy conversions.");
                    return Boolean.FALSE;
                }
            } else {
                if (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                    // Legacy to Legacy
                    // Non-Transactional, Managed (for ownership)
                    transferSpec.setMakeNonTransactional(Boolean.TRUE);
                } else {
                    // Non Legacy to Non Legacy
                    // External w/ Ownership
                    transferSpec.setMakeExternal(Boolean.TRUE);
                    transferSpec.setTakeOwnership(Boolean.TRUE);
                }
            }
        } else {
            // Storage will be used by right.  So don't let Transfer table own data.
            transferSpec.setMakeNonTransactional(Boolean.TRUE);
            if (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()
                    != hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
                if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                    // Don't support non-legacy to legacy conversions.
                    let.addIssue("Don't support Non-Legacy to Legacy conversions.");
                    return Boolean.FALSE;
                } else {
                    if (TableUtils.isACID(let) && hmsMirrorConfig.getMigrateACID().isDowngrade()) {
                        if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                            transferSpec.setMakeExternal(Boolean.TRUE);
                            transferSpec.setTakeOwnership(Boolean.FALSE);
                        } else {
                            transferSpec.setMakeExternal(Boolean.TRUE);
                        }
                    } else {
                        transferSpec.setMakeExternal(Boolean.TRUE);
                    }
                }
            } else {
                if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                    // Non Legacy to Non Legacy
                    // External w/ Ownership
                    transferSpec.setMakeExternal(Boolean.TRUE);
                    if (hmsMirrorConfig.getMigrateACID().isDowngrade()) {
                        // The location will be used by the right cluster, so do not own the data.
                        transferSpec.setTakeOwnership(Boolean.FALSE);
                    } else {
                        // Common Storage is used just like Intermediate Storage in this case.
                        transferSpec.setTakeOwnership(Boolean.TRUE);
                    }
                }
            }
        }

        transferSpec.setTableNamePrefix(hmsMirrorConfig.getTransfer().getTransferPrefix());
        transferSpec.setReplaceLocation(Boolean.TRUE);

        if (rtn)
            // Build transfer table.
            rtn = tableService.buildTableSchema(transferSpec);

        // Build Shadow Spec (don't build when using commonStorage)
        // If acid and ma.isOn
        // if not downgrade
        if ((hmsMirrorConfig.getTransfer().getIntermediateStorage() != null && !TableUtils.isACID(let)) ||
                (TableUtils.isACID(let) && hmsMirrorConfig.getMigrateACID().isOn())) {
            if (!hmsMirrorConfig.getMigrateACID().isDowngrade() ||
                    // Is Downgrade but the downgraded location isn't available to the right.
                    (hmsMirrorConfig.getMigrateACID().isDowngrade()
                            && hmsMirrorConfig.getTransfer().getCommonStorage() == null)) {
                if (!hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
                    CopySpec shadowSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.SHADOW);
                    shadowSpec.setUpgrade(Boolean.TRUE);
                    shadowSpec.setMakeExternal(Boolean.TRUE);
                    shadowSpec.setTakeOwnership(Boolean.FALSE);
                    shadowSpec.setReplaceLocation(Boolean.TRUE);
                    shadowSpec.setTableNamePrefix(hmsMirrorConfig.getTransfer().getShadowPrefix());

                    if (rtn)
                        rtn = tableService.buildTableSchema(shadowSpec);
                }
            }
        }
        return rtn;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

        log.debug("Table: {} buildout Intermediate SQL", tableMirror.getName());

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable tet = tableMirror.getEnvironmentTable(Environment.TRANSFER);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);
        EnvironmentTable set = tableMirror.getEnvironmentTable(Environment.SHADOW);

        ret.getSql().clear();

        // LEFT Transfer Table
        database = tableMirror.getParent().getName();
        useDb = MessageFormat.format(MirrorConf.USE, database);

        let.addSql(TableUtils.USE_DESC, useDb);
        // Drop any previous TRANSFER table, if it exists.
        String transferDropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, tet.getName());
        let.addSql(TableUtils.DROP_DESC, transferDropStmt);

        // Create Transfer Table
        String transferCreateStmt = tableService.getCreateStatement(tableMirror, Environment.TRANSFER);
        let.addSql(TableUtils.CREATE_TRANSFER_DESC, transferCreateStmt);

        database = getHmsMirrorCfgService().getResolvedDB(tableMirror.getParent().getName());
        useDb = MessageFormat.format(MirrorConf.USE, database);
        ret.addSql(TableUtils.USE_DESC, useDb);

        // RIGHT SHADOW Table
        if (!set.getDefinition().isEmpty()) { //config.getTransfer().getCommonStorage() == null || !config.getMigrateACID().isDowngrade()) {

            // Drop any previous SHADOW table, if it exists.
            String dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
            ret.addSql(TableUtils.DROP_DESC, dropStmt);
            // Create Shadow Table
            String shadowCreateStmt = tableService.getCreateStatement(tableMirror, Environment.SHADOW);
            ret.addSql(TableUtils.CREATE_SHADOW_DESC, shadowCreateStmt);
            // Repair Partitions
//            if (let.getPartitioned()) {
//                String shadowMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, set.getName());
//                ret.addSql(TableUtils.REPAIR_DESC, shadowMSCKStmt);
//            }
        }

        // RIGHT Final Table
        String rightDrop = null;
        switch (ret.getCreateStrategy()) {
            case NOTHING:
            case LEAVE:
                // Do Nothing.
                break;
            case DROP:
                rightDrop = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(TableUtils.DROP_DESC, rightDrop);
                break;
            case REPLACE:
                rightDrop = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(TableUtils.DROP_DESC, rightDrop);
                String createStmt = tableService.getCreateStatement(tableMirror, Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt);
                break;
            case CREATE:
                String createStmt2 = tableService.getCreateStatement(tableMirror, Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()
                        && hmsMirrorConfig.isTransferOwnership() && let.getOwner() != null) {
                    String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, ret.getName(), let.getOwner());
                    ret.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
                }
                if (let.getPartitioned()) {
                    if (getHmsMirrorCfgService().getHmsMirrorConfig().getTransfer().getCommonStorage() != null) {
                        if (!TableUtils.isACID(let) || (TableUtils.isACID(let) && hmsMirrorConfig.getMigrateACID().isDowngrade())) {
                            String rightMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, ret.getName());
                            ret.addSql(TableUtils.REPAIR_DESC, rightMSCKStmt);
                        }
                    }
                }

                break;
        }

        rtn = Boolean.TRUE;
        return rtn;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

        rtn = buildOutDefinition(tableMirror);
        if (rtn)
            rtn = buildOutSql(tableMirror);

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable tet = tableMirror.getEnvironmentTable(Environment.TRANSFER);
        EnvironmentTable set = tableMirror.getEnvironmentTable(Environment.SHADOW);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        if (rtn) {
            // Construct Transfer SQL
            if (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()
                    && !hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
                // We need to ensure that 'tez' is the execution engine.
                let.addSql(new Pair(TEZ_EXECUTION_DESC, SET_TEZ_AS_EXECUTION_ENGINE));
            }

            Pair cleanUp = new Pair("Post Migration Cleanup", "-- To be run AFTER final RIGHT SQL statements.");
            let.addCleanUpSql(cleanUp);

            String useLeftDb = MessageFormat.format(MirrorConf.USE, tableMirror.getParent().getName());
            Pair leftUsePair = new Pair(TableUtils.USE_DESC, useLeftDb);
            let.addCleanUpSql(leftUsePair);

            rtn = tableService.buildTransferSql(tableMirror, Environment.LEFT, Environment.TRANSFER, Environment.RIGHT);
            //tableMirror.buildTransferSql(let, set, ret, config);

            // Execute the LEFT sql if config.execute.
            if (rtn) {
                rtn = tableService.runTableSql(tableMirror, Environment.LEFT);
            }

            // Execute the RIGHT sql if config.execute.
            if (rtn) {
                rtn = tableService.runTableSql(tableMirror, Environment.RIGHT);
            }

            if (rtn) {
                // Run the Cleanup Scripts
                tableService.runTableSql(let.getCleanUpSql(), tableMirror, Environment.LEFT);
            }

            // RIGHT Shadow table
//            if (set.getDefinition().size() > 0) {
//                List<Pair> rightCleanup = new ArrayList<Pair>();
//
//                String useRightDb = MessageFormat.format(MirrorConf.USE, config.getResolvedDB(dbMirror.getName()));
//                Pair rightUsePair = new Pair(TableUtils.USE_DESC, useRightDb);
//                ret.addCleanUpSql(rightUsePair);
//                String rightDropShadow = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
//                Pair rightDropPair = new Pair(TableUtils.DROP_SHADOW_TABLE, rightDropShadow);
//                ret.addCleanUpSql(rightDropPair);
//                tblMirror.addStep("RIGHT ACID Shadow SQL Cleanup", "Built");
//
//                if (rtn) {
//                    // Run the Cleanup Scripts
//                    config.getCluster(Environment.RIGHT).runTableSql(ret.getCleanUpSql(), tblMirror, Environment.RIGHT);
//                }
//            }
        }
        return rtn;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }
}
