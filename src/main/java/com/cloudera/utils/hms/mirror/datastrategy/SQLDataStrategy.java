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
import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.HmsMirrorConfigUtil;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.TableService;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Slf4j
@Getter
public class SQLDataStrategy extends DataStrategyBase implements DataStrategy {

    private ConfigService configService;

    private SQLAcidDowngradeInPlaceDataStrategy sqlAcidDowngradeInPlaceDataStrategy;
    private TableService tableService;
    private IntermediateDataStrategy intermediateDataStrategy;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    public SQLDataStrategy(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) throws RequiredConfigurationException {
        Boolean rtn = Boolean.FALSE;
        log.debug("Table: {} buildout SQL Definition", tableMirror.getName());
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
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);

        if (tableService.isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
            rtn = getSqlAcidDowngradeInPlaceDataStrategy().execute(tableMirror);
        } else if (!isBlank(hmsMirrorConfig.getTransfer().getIntermediateStorage())
                || !isBlank(hmsMirrorConfig.getTransfer().getTargetNamespace())
                || (TableUtils.isACID(let)
                && hmsMirrorConfig.getMigrateACID().isOn())) {
            if (TableUtils.isACID(let)) {
                tableMirror.setStrategy(DataStrategyEnum.ACID);
            }
            rtn = getIntermediateDataStrategy().execute(tableMirror);
        } else {

            EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);
            EnvironmentTable set = getEnvironmentTable(Environment.SHADOW, tableMirror);

            // We should not get ACID tables in this routine.
            try {
                rtn = buildOutDefinition(tableMirror);
            } catch (RequiredConfigurationException e) {
                let.addIssue("Failed to build out definition: " + e.getMessage());
                rtn = Boolean.FALSE;
            }

            if (rtn)
                rtn = AVROCheck(tableMirror);

            if (rtn) {
                try {
                    rtn = buildOutSql(tableMirror);
                } catch (MissingDataPointException e) {
                    let.addIssue("Failed to build out SQL: " + e.getMessage());
                    rtn = Boolean.FALSE;
                }
            }

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
