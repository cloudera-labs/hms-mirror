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

import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.SCHEMA_EXISTS_NO_ACTION_DATA;

public class CommonDataStrategy extends DataStrategyBase implements DataStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(CommonDataStrategy.class);

    @Override
    public Boolean execute() {
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        if (TableUtils.isACID(let)) {
            rtn = Boolean.FALSE;
            tableMirror.addIssue(Environment.RIGHT,
                    "Can't transfer SCHEMA reference on COMMON storage for ACID tables.");
        } else {
            rtn = buildOutDefinition();
        }

        if (rtn) {
            rtn = buildOutSql();
        }
        // Execute the RIGHT sql if config.execute.
        if (rtn) {
            rtn = config.getCluster(Environment.RIGHT).runTableSql(tableMirror);
        }

        return rtn;
    }

    @Override
    public Boolean buildOutDefinition() {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout COMMON Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        // Can't LINK ACID tables.
        if (TableUtils.isHiveNative(let) && !TableUtils.isACID(let)) {
            // Swap out the namespace of the LEFT with the RIGHT.
            copySpec.setReplaceLocation(Boolean.FALSE);
            if (config.convertManaged())
                copySpec.setUpgrade(Boolean.TRUE);
            // COMMON owns the data unless readonly specified.
            if (!config.isReadOnly())
                copySpec.setTakeOwnership(Boolean.TRUE);
            if (config.isNoPurge())
                copySpec.setTakeOwnership(Boolean.FALSE);

            if (config.isSync()) {
                // We assume that the 'definitions' are only there if the
                //     table exists.
                if (!let.getExists() && ret.getExists()) {
                    // If left is empty and right is not, DROP RIGHT.
                    ret.addIssue("Schema doesn't exist in 'source'.  Will be DROPPED.");
                    ret.setCreateStrategy(CreateStrategy.DROP);
                } else if (let.getExists() && !ret.getExists()) {
                    // If left is defined and right is not, CREATE RIGHT.
                    ret.addIssue("Schema missing, will be CREATED");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                } else if (let.getExists() && ret.getExists()) {
                    // If left and right, check schema change and replace if necessary.
                    // Compare Schemas.
                    if (tableMirror.schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                        ret.addIssue(SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
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
                // With sync, don't own data.
                copySpec.setTakeOwnership(Boolean.FALSE);
            } else {
                if (ret.getExists()) {
                    // Already exists, no action.
                    ret.addIssue(SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                    return Boolean.FALSE;
                } else {
                    ret.addIssue("Schema will be created");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                }
            }
            // Rebuild Target from Source.
            rtn = tableMirror.buildTableSchema(copySpec);
        } else {
            let.addIssue("Can't use COMMON for ACID tables");
            ret.setCreateStrategy(CreateStrategy.NOTHING);
        }
        return rtn;
    }

    @Override
    public Boolean buildOutSql() {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + getDBMirror().getName() + " buildout COMMON SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        //ret.getSql().clear();

        database = config.getResolvedDB(dbMirror.getName());
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
                String createStmt = tableMirror.getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt);
                break;
            case CREATE:
                ret.addSql(TableUtils.USE_DESC, useDb);
                String createStmt2 = tableMirror.getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                if (!config.getCluster(Environment.RIGHT).getLegacyHive() && config.getTransferOwnership() && let.getOwner() != null) {
                    String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, ret.getName(), let.getOwner());
                    ret.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
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
            if (config.getEvaluatePartitionLocation()) {
                // TODO: Write out the SQL to build the partitions.  NOTE: We need to get the partition locations and modify them
                //       to the new namespace.
                String tableParts = config.getTranslator().buildPartitionAddStatement(ret);
                String addPartSql = MessageFormat.format(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION, ret.getName(), tableParts);
                ret.addSql(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION_DESC, addPartSql);
            } else if (config.getCluster(Environment.RIGHT).getPartitionDiscovery().getInitMSCK()) {
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
}
