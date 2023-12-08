/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hadoop.hms.mirror.datastrategy;

import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;

import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hadoop.hms.mirror.SessionVars.SET_TEZ_AS_EXECUTION_ENGINE;
import static com.cloudera.utils.hadoop.hms.mirror.SessionVars.TEZ_EXECUTION_DESC;

public class IntermediateDataStrategy extends DataStrategyBase implements DataStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(IntermediateDataStrategy.class);
    @Override
    public Boolean execute() {
        Boolean rtn = Boolean.FALSE;

        rtn = tableMirror.buildoutIntermediateDefinition(config, dbMirror);
        if (rtn)
            rtn = tableMirror.buildoutIntermediateSql(config, dbMirror);

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable tet = tableMirror.getEnvironmentTable(Environment.TRANSFER);
        EnvironmentTable set = tableMirror.getEnvironmentTable(Environment.SHADOW);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        if (rtn) {
            // Construct Transfer SQL
            if (config.getCluster(Environment.LEFT).getLegacyHive() && !config.getTransfer().getStorageMigration().isDistcp()) {
                // We need to ensure that 'tez' is the execution engine.
                let.addSql(new Pair(TEZ_EXECUTION_DESC, SET_TEZ_AS_EXECUTION_ENGINE));
            }

            Pair cleanUp = new Pair("Post Migration Cleanup", "-- To be run AFTER final RIGHT SQL statements.");
            let.addCleanUpSql(cleanUp);

            String useLeftDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
            Pair leftUsePair = new Pair(TableUtils.USE_DESC, useLeftDb);
            let.addCleanUpSql(leftUsePair);

            rtn = tableMirror.buildTransferSql(let, set, ret, config);

            // Execute the LEFT sql if config.execute.
            if (rtn) {
                rtn = config.getCluster(Environment.LEFT).runTableSql(tableMirror);
            }

            // Execute the RIGHT sql if config.execute.
            if (rtn) {
                rtn = config.getCluster(Environment.RIGHT).runTableSql(tableMirror);
            }

            if (rtn) {
                // Run the Cleanup Scripts
                config.getCluster(Environment.LEFT).runTableSql(let.getCleanUpSql(), tableMirror, Environment.LEFT);
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

    @Override
    public Boolean buildOutDefinition() {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + tableMirror.getName() + " buildout Intermediate Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        CopySpec rightSpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);

        if (ret.getExists()) {
            if (config.getCluster(Environment.RIGHT).getCreateIfNotExists() && config.isSync()) {
                ret.addIssue(CINE_WITH_EXIST.getDesc());
                ret.setCreateStrategy(CreateStrategy.CREATE);
            } else if(TableUtils.isACID(ret) && config.isSync()) {
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
            if (config.getMigrateACID().isDowngrade()) {
                if (config.getTransfer().getCommonStorage() == null) {
                    if (config.getTransfer().getStorageMigration().isDistcp()) {
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
        rtn = tableMirror.buildTableSchema(rightSpec);

        // Build Transfer Spec.
        CopySpec transferSpec = new CopySpec(config, Environment.LEFT, Environment.TRANSFER);
        if (config.getTransfer().getCommonStorage() == null) {
            if (config.getCluster(Environment.LEFT).getLegacyHive() != config.getCluster(Environment.RIGHT).getLegacyHive()) {
                if (config.getCluster(Environment.LEFT).getLegacyHive()) {
                    transferSpec.setMakeNonTransactional(Boolean.TRUE);
                } else {
                    // Don't support non-legacy to legacy conversions.
                    let.addIssue("Don't support Non-Legacy to Legacy conversions.");
                    return Boolean.FALSE;
                }
            } else {
                if (config.getCluster(Environment.LEFT).getLegacyHive()) {
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
            if (config.getCluster(Environment.LEFT).getLegacyHive() != config.getCluster(Environment.RIGHT).getLegacyHive()) {
                if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                    // Don't support non-legacy to legacy conversions.
                    let.addIssue("Don't support Non-Legacy to Legacy conversions.");
                    return Boolean.FALSE;
                } else {
                    if (TableUtils.isACID(let) && config.getMigrateACID().isDowngrade()) {
                        if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
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
                if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                    // Non Legacy to Non Legacy
                    // External w/ Ownership
                    transferSpec.setMakeExternal(Boolean.TRUE);
                    if (config.getMigrateACID().isDowngrade()) {
                        // The location will be used by the right cluster, so do not own the data.
                        transferSpec.setTakeOwnership(Boolean.FALSE);
                    } else {
                        // Common Storage is used just like Intermediate Storage in this case.
                        transferSpec.setTakeOwnership(Boolean.TRUE);
                    }
                }
            }
        }

        transferSpec.setTableNamePrefix(config.getTransfer().getTransferPrefix());
        transferSpec.setReplaceLocation(Boolean.TRUE);

        if (rtn)
            // Build transfer table.
            rtn = tableMirror.buildTableSchema(transferSpec);

        // Build Shadow Spec (don't build when using commonStorage)
        // If acid and ma.isOn
        // if not downgrade
        if ((config.getTransfer().getIntermediateStorage() != null && !TableUtils.isACID(let)) ||
                (TableUtils.isACID(let) && config.getMigrateACID().isOn())) {
            if (!config.getMigrateACID().isDowngrade() ||
                    // Is Downgrade but the downgraded location isn't available to the right.
                    (config.getMigrateACID().isDowngrade() && config.getTransfer().getCommonStorage() == null)) {
                if (!config.getTransfer().getStorageMigration().isDistcp()) { // ||
//                (config.getMigrateACID().isOn() && TableUtils.isACID(let)
//                        && !config.getMigrateACID().isDowngrade())) {
                    CopySpec shadowSpec = new CopySpec(config, Environment.LEFT, Environment.SHADOW);
                    shadowSpec.setUpgrade(Boolean.TRUE);
                    shadowSpec.setMakeExternal(Boolean.TRUE);
                    shadowSpec.setTakeOwnership(Boolean.FALSE);
                    shadowSpec.setReplaceLocation(Boolean.TRUE);
                    shadowSpec.setTableNamePrefix(config.getTransfer().getShadowPrefix());

                    if (rtn)
                        rtn = tableMirror.buildTableSchema(shadowSpec);
                }
            }
        }
        return rtn;
    }

    @Override
    public Boolean buildOutSql() {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + tableMirror.getName() + " buildout Intermediate SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable tet = getEnvironmentTable(Environment.TRANSFER);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);
        EnvironmentTable set = getEnvironmentTable(Environment.SHADOW);

        ret.getSql().clear();

        // LEFT Transfer Table
        database = dbMirror.getName();
        useDb = MessageFormat.format(MirrorConf.USE, database);

        let.addSql(TableUtils.USE_DESC, useDb);
        // Drop any previous TRANSFER table, if it exists.
        String transferDropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, tet.getName());
        let.addSql(TableUtils.DROP_DESC, transferDropStmt);

        // Create Transfer Table
        String transferCreateStmt = tableMirror.getCreateStatement(Environment.TRANSFER);
        let.addSql(TableUtils.CREATE_TRANSFER_DESC, transferCreateStmt);

        database = config.getResolvedDB(dbMirror.getName());
        useDb = MessageFormat.format(MirrorConf.USE, database);
        ret.addSql(TableUtils.USE_DESC, useDb);

        // RIGHT SHADOW Table
        if (set.getDefinition().size() > 0) { //config.getTransfer().getCommonStorage() == null || !config.getMigrateACID().isDowngrade()) {

            // Drop any previous SHADOW table, if it exists.
            String dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
            ret.addSql(TableUtils.DROP_DESC, dropStmt);
            // Create Shadow Table
            String shadowCreateStmt = tableMirror.getCreateStatement(Environment.SHADOW);
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
                String createStmt = tableMirror.getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt);
                break;
            case CREATE:
                String createStmt2 = tableMirror.getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                if (!config.getCluster(Environment.RIGHT).getLegacyHive() && config.getTransferOwnership() && let.getOwner() != null) {
                    String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, ret.getName(), let.getOwner());
                    ret.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
                }
                if (let.getPartitioned()) {
                    if (config.getTransfer().getCommonStorage() != null) {
                        if (!TableUtils.isACID(let) || (TableUtils.isACID(let) && config.getMigrateACID().isDowngrade())) {
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
}
