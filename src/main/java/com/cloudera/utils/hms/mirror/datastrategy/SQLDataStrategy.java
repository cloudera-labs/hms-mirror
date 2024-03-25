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
import com.cloudera.utils.hms.mirror.service.TableService;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

import static com.cloudera.utils.hms.mirror.MessageCode.*;

@Component
@Slf4j
public class SQLDataStrategy extends DataStrategyBase implements DataStrategy {

    @Getter
    private SQLAcidDowngradeInPlaceDataStrategy sqlAcidDowngradeInPlaceDataStrategy;

    @Getter
    private TableService tableService;

    @Getter
    private IntermediateDataStrategy intermediateDataStrategy;

    public SQLDataStrategy(ConfigService configService) {
        this.configService = configService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        log.debug("Table: " + tableMirror.getName() + " buildout SQL Definition");
        Config config = getConfigService().getConfig();


        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        EnvironmentTable set = null;

        let = getEnvironmentTable(Environment.LEFT, tableMirror);
        ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        // Different transfer technique.  Staging location.
        if (config.getTransfer().getIntermediateStorage() != null ||
                config.getTransfer().getCommonStorage() != null ||
                TableUtils.isACID(let)) {
            return getIntermediateDataStrategy().buildOutDefinition(tableMirror);
        }

        if (ret.isExists()) {
            if (config.isSync() && config.getCluster(Environment.RIGHT).isCreateIfNotExists()) {
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
            shadowSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.SHADOW);

            if (config.convertManaged())
                shadowSpec.setUpgrade(Boolean.TRUE);

            // Don't claim data.  It will be in the LEFT cluster, so the LEFT owns it.
            shadowSpec.setTakeOwnership(Boolean.FALSE);

            // Create table with alter name in RIGHT cluster.
            shadowSpec.setTableNamePrefix(config.getTransfer().getShadowPrefix());

            // Build Shadow from Source.
            rtn = tableService.buildTableSchema(shadowSpec);
        }

        // Create final table in right.
        CopySpec rightSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);

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
        rtn = tableService.buildTableSchema(rightSpec);

        return rtn;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        log.debug("Table: " + tableMirror.getName() + " buildout SQL SQL");
        Config config = getConfigService().getConfig();

        if (config.getTransfer().getIntermediateStorage() != null ||
                config.getTransfer().getCommonStorage() != null) {
            return getIntermediateDataStrategy().buildOutSql(tableMirror);
        }

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);
        EnvironmentTable set = getEnvironmentTable(Environment.SHADOW, tableMirror);

        ret.getSql().clear();

        if (TableUtils.isACID(let)) {
            rtn = Boolean.FALSE;
            // TODO: Hum... Not sure this is right.
            tableMirror.addIssue(Environment.LEFT, "Shouldn't get an ACID table here.");
        } else {
            database = getConfigService().getResolvedDB(tableMirror.getParent().getName());
            useDb = MessageFormat.format(MirrorConf.USE, database);

            ret.addSql(TableUtils.USE_DESC, useDb);

            String dropStmt = null;
            // Create RIGHT Shadow Table
            if (!set.getDefinition().isEmpty()) {
                // Drop any previous SHADOW table, if it exists.
                dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);

                String shadowCreateStmt = tableService.getCreateStatement(tableMirror, Environment.SHADOW);
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
                    String createStmt = tableService.getCreateStatement(tableMirror, Environment.RIGHT);
                    //tableMirror.getCreateStatement(Environment.RIGHT);
                    ret.addSql(TableUtils.CREATE_DESC, createStmt);
                    break;
                case CREATE:
                    String createStmt2 = tableService.getCreateStatement(tableMirror, Environment.RIGHT);
                    //tableMirror.getCreateStatement(Environment.RIGHT);
                    ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                    if (!config.getCluster(Environment.RIGHT).isLegacyHive() && config.isTransferOwnership() && let.getOwner() != null) {
                        String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, ret.getName(), let.getOwner());
                        ret.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
                    }
                    break;
            }
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        Config config = getConfigService().getConfig();

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);

        if (tableService.isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
            rtn = getSqlAcidDowngradeInPlaceDataStrategy().execute(tableMirror);
        } else if (config.getTransfer().getIntermediateStorage() != null
                || config.getTransfer().getCommonStorage() != null
                || (TableUtils.isACID(let)
                && config.getMigrateACID().isOn())) {
            if (TableUtils.isACID(let)) {
                tableMirror.setStrategy(DataStrategyEnum.ACID);
            }
            rtn = getIntermediateDataStrategy().execute(tableMirror);
        } else {

            EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);
            EnvironmentTable set = getEnvironmentTable(Environment.SHADOW, tableMirror);

            // We should not get ACID tables in this routine.
            rtn = buildOutDefinition(tableMirror);

            if (rtn)
                rtn = AVROCheck(tableMirror);

            if (rtn)
                rtn = buildOutSql(tableMirror);

            // Construct Transfer SQL
            if (rtn) {
                // TODO: Double check this...
                rtn = tableService.buildTransferSql(tableMirror, Environment.TRANSFER, Environment.SHADOW, Environment.RIGHT);

                // Execute the RIGHT sql if config.execute.
                if (rtn) {
                    tableService.runTableSql(tableMirror, Environment.RIGHT);
                }
            }
        }
        return rtn;
    }

    @Autowired
    public void setIntermediateDataStrategy(IntermediateDataStrategy intermediateDataStrategy) {
        this.intermediateDataStrategy = intermediateDataStrategy;
    }

    @Autowired
    public void setSqlAcidDowngradeInPlaceDataStrategy(SQLAcidDowngradeInPlaceDataStrategy sqlAcidDowngradeInPlaceDataStrategy) {
        this.sqlAcidDowngradeInPlaceDataStrategy = sqlAcidDowngradeInPlaceDataStrategy;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }
}
