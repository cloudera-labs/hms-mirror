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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.text.MessageFormat;

import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.*;

public class SQLDataStrategy extends DataStrategyBase implements DataStrategy {
    private static final Logger LOG = LogManager.getLogger(SQLDataStrategy.class);
    @Override
    public Boolean execute() {
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        if (tableMirror.isACIDDowngradeInPlace(config, let)) {
            DataStrategy dsACIDDowngradeInplace = DataStrategyEnum.SQL_ACID_DOWNGRADE_INPLACE.getDataStrategy();
            dsACIDDowngradeInplace.setTableMirror(tableMirror);
            dsACIDDowngradeInplace.setDBMirror(dbMirror);
            dsACIDDowngradeInplace.setConfig(config);
            rtn = dsACIDDowngradeInplace.execute();//doSQLACIDDowngradeInplace();
        } else if (config.getTransfer().getIntermediateStorage() != null
                || config.getTransfer().getCommonStorage() != null
                || (TableUtils.isACID(let)
                && config.getMigrateACID().isOn())) {
            if (TableUtils.isACID(let)) {
                tableMirror.setStrategy(DataStrategyEnum.ACID);
            }
            DataStrategy dsIt = DataStrategyEnum.INTERMEDIATE.getDataStrategy();
            dsIt.setTableMirror(tableMirror);
            dsIt.setDBMirror(dbMirror);
            dsIt.setConfig(config);
            rtn = dsIt.execute();//doIntermediateTransfer();
        } else {

            EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);
            EnvironmentTable set = tableMirror.getEnvironmentTable(Environment.SHADOW);

            // We should not get ACID tables in this routine.
            rtn = buildOutDefinition();//tableMirror.buildoutSQLDefinition(config, dbMirror);

            if (rtn)
                rtn = AVROCheck();

            if (rtn)
                rtn = buildOutSql();//tableMirror.buildoutSQLSql(config, dbMirror);

            // Construct Transfer SQL
            if (rtn) {
//                DataStrategy dsIt = DataStrategyEnum.INTERMEDIATE.getDataStrategy();
//                dsIt.setTableMirror(tableMirror);
//                dsIt.setDBMirror(dbMirror);
//                dsIt.setConfig(config);
//                rtn = dsIt.buildOutSql();
                rtn = tableMirror.buildTransferSql(let, set, ret, config);

                // Execute the RIGHT sql if config.execute.
                if (rtn) {
                    config.getCluster(Environment.RIGHT).runTableSql(tableMirror);
                }
            }
        }
        return rtn;
    }

    @Override
    public Boolean buildOutDefinition() {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + tableMirror.getName() + " buildout SQL Definition");


        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        EnvironmentTable set = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        // Different transfer technique.  Staging location.
        if (config.getTransfer().getIntermediateStorage() != null ||
                config.getTransfer().getCommonStorage() != null ||
                TableUtils.isACID(let)) {
            DataStrategy dsIt = DataStrategyEnum.INTERMEDIATE.getDataStrategy();
            dsIt.setTableMirror(tableMirror);
            dsIt.setDBMirror(dbMirror);
            dsIt.setConfig(config);
            return dsIt.buildOutDefinition();
//            return buildoutIntermediateDefinition(config, dbMirror);
        }

        if (ret.getExists()) {
            if (config.isSync() && config.getCluster(Environment.RIGHT).getCreateIfNotExists()) {
                // sync with overwrite.
                ret.addIssue(SQL_SYNC_W_CINE.getDesc());
                ret.setCreateStrategy(CreateStrategy.CREATE);
            } else {
                ret.addIssue(SCHEMA_EXISTS_NO_ACTION.getDesc());
                return Boolean.FALSE;
            }
        } else {
            ret.addIssue(SCHEMA_WILL_BE_CREATED.getDesc());
            ret.setCreateStrategy(CreateStrategy.CREATE);
        }

        if (config.getTransfer().getCommonStorage() == null) {
            CopySpec shadowSpec = null;

            // Create a 'shadow' table definition on right cluster pointing to the left data.
            shadowSpec = new CopySpec(config, Environment.LEFT, Environment.SHADOW);

            if (config.convertManaged())
                shadowSpec.setUpgrade(Boolean.TRUE);

            // Don't claim data.  It will be in the LEFT cluster, so the LEFT owns it.
            shadowSpec.setTakeOwnership(Boolean.FALSE);

            // Create table with alter name in RIGHT cluster.
            shadowSpec.setTableNamePrefix(config.getTransfer().getShadowPrefix());

            // Build Shadow from Source.
            rtn = tableMirror.buildTableSchema(shadowSpec);
        }

        // Create final table in right.
        CopySpec rightSpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);

        // Swap out the namespace of the LEFT with the RIGHT.
        rightSpec.setReplaceLocation(Boolean.TRUE);
        if (TableUtils.isManaged(let) && config.convertManaged()) {
            rightSpec.setUpgrade(Boolean.TRUE);
        } else {
            rightSpec.setMakeExternal(Boolean.TRUE);
        }
        if (config.isReadOnly()) {
            rightSpec.setTakeOwnership(Boolean.FALSE);
        } else if (TableUtils.isManaged(let)) {
            rightSpec.setTakeOwnership(Boolean.TRUE);
        }
        if (config.isNoPurge()) {
            rightSpec.setTakeOwnership(Boolean.FALSE);
        }

        // Rebuild Target from Source.
        rtn = tableMirror.buildTableSchema(rightSpec);

        return rtn;
    }

    @Override
    public Boolean buildOutSql() {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + tableMirror.getName() + " buildout SQL SQL");

        if (config.getTransfer().getIntermediateStorage() != null ||
                config.getTransfer().getCommonStorage() != null) {
            DataStrategy dsIt = DataStrategyEnum.INTERMEDIATE.getDataStrategy();
            dsIt.setTableMirror(tableMirror);
            dsIt.setDBMirror(dbMirror);
            dsIt.setConfig(config);
            return dsIt.buildOutSql();
//            return buildoutIntermediateSql(config, dbMirror);
        }

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);
        EnvironmentTable set = getEnvironmentTable(Environment.SHADOW);

        ret.getSql().clear();

        if (TableUtils.isACID(let)) {
            rtn = Boolean.FALSE;
            // TODO: Hum... Not sure this is right.
            tableMirror.addIssue(Environment.LEFT, "Shouldn't get an ACID table here.");
        } else {
//        if (!isACIDDowngradeInPlace(config, let)) {
            database = config.getResolvedDB(dbMirror.getName());
            useDb = MessageFormat.format(MirrorConf.USE, database);

            ret.addSql(TableUtils.USE_DESC, useDb);

            String dropStmt = null;
            // Create RIGHT Shadow Table
            if (set.getDefinition().size() > 0) {
                // Drop any previous SHADOW table, if it exists.
                dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);

                String shadowCreateStmt = tableMirror.getCreateStatement(Environment.SHADOW);
                ret.addSql(TableUtils.CREATE_SHADOW_DESC, shadowCreateStmt);
                // Repair Partitions
//                if (let.getPartitioned()) {
//                    String shadowMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, set.getName());
//                    ret.addSql(TableUtils.REPAIR_DESC, shadowMSCKStmt);
//                }
            }

            // RIGHT Final Table
            switch (ret.getCreateStrategy()) {
                case NOTHING:
                case LEAVE:
                    // Do Nothing.
                    break;
                case DROP:
                    dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                    ret.addSql(TableUtils.DROP_DESC, dropStmt);
                    break;
                case REPLACE:
                    dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                    ret.addSql(TableUtils.DROP_DESC, dropStmt);
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
                    break;
            }

            rtn = Boolean.TRUE;
        }
        return rtn;
    }
}
