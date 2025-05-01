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
import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
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
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Slf4j
@Getter
public class SQLDataStrategy extends DataStrategyBase {

    private final ConfigService configService;
    private final SQLAcidInPlaceDataStrategy sqlAcidInPlaceDataStrategy;
    private final TableService tableService;
    private final IntermediateDataStrategy intermediateDataStrategy;

    public SQLDataStrategy(StatsCalculatorService statsCalculatorService,
                           ExecuteSessionService executeSessionService,
                           TranslatorService translatorService,
                           ConfigService configService,
                           SQLAcidInPlaceDataStrategy sqlAcidInPlaceDataStrategy,
                           TableService tableService,
                           IntermediateDataStrategy intermediateDataStrategy) {
        super(statsCalculatorService, executeSessionService, translatorService);
        this.configService = configService;
        this.sqlAcidInPlaceDataStrategy = sqlAcidInPlaceDataStrategy;
        this.tableService = tableService;
        this.intermediateDataStrategy = intermediateDataStrategy;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) throws RequiredConfigurationException {
        Boolean rtn = Boolean.FALSE;
        log.debug("Table: {}.{} buildout SQL Definition", tableMirror.getParent().getName(), tableMirror.getName());
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        EnvironmentTable set = null;

        let = getEnvironmentTable(Environment.LEFT, tableMirror);
        ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        // Different transfer technique.  Staging location.
        if (!isBlank(config.getTransfer().getIntermediateStorage()) ||
                !isBlank(config.getTransfer().getTargetNamespace()) ||
                TableUtils.isACID(let)) {
            return getIntermediateDataStrategy().buildOutDefinition(tableMirror);
        }

        if (ret.isExists()) {
            if (let.isExists()) {
                if (config.isSync() && config.getCluster(Environment.RIGHT).isCreateIfNotExists()) {
                    // sync with overwrite.
                    ret.addIssue(SQL_SYNC_W_CINE.getDesc());
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                } else {
                    ret.addIssue(SCHEMA_EXISTS_NO_ACTION.getDesc());
                    ret.addSql(SKIPPED.getDesc(), "-- " + SCHEMA_EXISTS_NO_ACTION.getDesc());
                    String msg = MessageFormat.format(TABLE_ISSUE.getDesc(), tableMirror.getParent().getName(), tableMirror.getName(),
                            SCHEMA_EXISTS_NO_ACTION.getDesc());
                    log.error(msg);
                    return Boolean.FALSE;
                }
            } else {
                ret.addIssue(SCHEMA_EXISTS_TARGET_MISMATCH.getDesc());
                if (config.isSync()) {
                    ret.setCreateStrategy(CreateStrategy.DROP);
                } else {
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                }
                return Boolean.TRUE;
            }
        } else {
            ret.addIssue(SCHEMA_WILL_BE_CREATED.getDesc());
            ret.setCreateStrategy(CreateStrategy.CREATE);
        }

        if (!isBlank(config.getTargetNamespace())) {
            // If the temp cluster doesn't exist, create it as a clone of the LEFT.
            if (isNull(config.getCluster(Environment.SHADOW))) {
                Cluster shadowCluster = config.getCluster(Environment.LEFT).clone();
                config.getClusters().put(Environment.SHADOW, shadowCluster);
            }

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
            rtn = buildTableSchema(shadowSpec);
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
        rtn = buildTableSchema(rightSpec);

        return rtn;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.FALSE;
        log.debug("Table: {} buildout SQL SQL", tableMirror.getName());
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        if (config.getTransfer().getIntermediateStorage() != null ||
                config.getTransfer().getTargetNamespace() != null) {
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
            database = HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config);
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
//                 Repair Partitions
                // TODO: Need to add ALTER partitions here if we know them.
                if (let.getPartitioned()) {
                    String shadowMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, set.getName());
                    ret.addSql(TableUtils.REPAIR_DESC, shadowMSCKStmt);
                }
            }

            // RIGHT Final Table
            switch (ret.getCreateStrategy()) {
                case NOTHING:
                case LEAVE:
                    // Do Nothing.
                    break;
                case DROP:
                    if (TableUtils.isView(ret))
                        dropStmt = MessageFormat.format(MirrorConf.DROP_VIEW, ret.getName());
                    else
                        dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                    ret.addSql(TableUtils.DROP_DESC, dropStmt);
                    break;
                case REPLACE:
                    if (TableUtils.isView(ret))
                        dropStmt = MessageFormat.format(MirrorConf.DROP_VIEW, ret.getName());
                    else
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
                    if (!config.getCluster(Environment.RIGHT).isLegacyHive() && config.getOwnershipTransfer().isTable() && let.getOwner() != null) {
                        String ownerSql = MessageFormat.format(MirrorConf.SET_TABLE_OWNER, ret.getName(), let.getOwner());
                        ret.addSql(MirrorConf.SET_TABLE_OWNER_DESC, ownerSql);
                    }
                    break;
            }
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    @Override
    public Boolean build(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);

        if (isACIDInPlace(tableMirror, Environment.LEFT)) {
            rtn = getSqlAcidInPlaceDataStrategy().build(tableMirror);
        } else if (!isBlank(config.getTransfer().getIntermediateStorage())
                || !isBlank(config.getTransfer().getTargetNamespace())
                || (TableUtils.isACID(let)
                && config.getMigrateACID().isOn())) {
            if (TableUtils.isACID(let)) {
                tableMirror.setStrategy(DataStrategyEnum.ACID);
            }
            rtn = getIntermediateDataStrategy().build(tableMirror);
        } else {

            EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);
            EnvironmentTable set = getEnvironmentTable(Environment.SHADOW, tableMirror);

            // We should not get ACID tables in this routine.
            try {
                rtn = buildOutDefinition(tableMirror);
            } catch (RequiredConfigurationException e) {
                let.addError("Failed to build out definition: " + e.getMessage());
                rtn = Boolean.FALSE;
            }

            if (rtn)
                rtn = AVROCheck(tableMirror);

            if (rtn) {
                try {
                    rtn = buildOutSql(tableMirror);
                } catch (MissingDataPointException e) {
                    let.addError("Failed to build out SQL: " + e.getMessage());
                    rtn = Boolean.FALSE;
                }
            }

            // Construct Transfer SQL
            if (rtn) {
                // TODO: Double check this...
                rtn = buildMigrationSql(tableMirror, Environment.LEFT, Environment.SHADOW, Environment.RIGHT);

            }
        }
        return rtn;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        log.info("SQLDataStrategy -> Table: {} execute", tableMirror.getName());
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        Boolean rtn = Boolean.FALSE;
        rtn = tableService.runTableSql(tableMirror, Environment.LEFT);
        if (rtn) {
            rtn = tableService.runTableSql(tableMirror, Environment.RIGHT);
        }
        // Run Cleanup Scripts on both sides.
        if (!config.isSaveWorkingTables()) {
            // Run the Cleanup Scripts
            boolean CleanupRtn = tableService.runTableSql(tableMirror.getEnvironmentTable(Environment.LEFT).getCleanUpSql(), tableMirror, Environment.LEFT);
            if (!CleanupRtn) {
                tableMirror.addIssue(Environment.LEFT, "Failed to run cleanup SQL.");
            }
            CleanupRtn = tableService.runTableSql(tableMirror.getEnvironmentTable(Environment.RIGHT).getCleanUpSql(), tableMirror, Environment.RIGHT);
            if (!CleanupRtn) {
                tableMirror.addIssue(Environment.RIGHT, "Failed to run cleanup SQL.");
            }
        }
        return rtn;
    }

}
