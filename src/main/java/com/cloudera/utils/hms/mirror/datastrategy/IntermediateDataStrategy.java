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
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
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
import static com.cloudera.utils.hms.mirror.SessionVars.SET_TEZ_AS_EXECUTION_ENGINE;
import static com.cloudera.utils.hms.mirror.SessionVars.TEZ_EXECUTION_DESC;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Slf4j
@Getter
public class IntermediateDataStrategy extends DataStrategyBase implements DataStrategy {

    private ConfigService configService;
    private TableService tableService;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    public IntermediateDataStrategy(ExecuteSessionService executeSessionService, TranslatorService translatorService) {
        this.executeSessionService = executeSessionService;
        this.translatorService = translatorService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) throws RequiredConfigurationException {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        BuildWhat buildWhat = whatToBuild(hmsMirrorConfig, tableMirror);

        log.debug("Table: {} buildout Intermediate Definition", tableMirror.getName());
        EnvironmentTable let = null;
        EnvironmentTable ret = null;

        let = getEnvironmentTable(Environment.LEFT, tableMirror);
        ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        if (buildWhat.rightTable) {
            CopySpec rightSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);

            if (ret.isExists()) {
                if (!TableUtils.isACID(ret) && hmsMirrorConfig.getCluster(Environment.RIGHT).isCreateIfNotExists() && hmsMirrorConfig.isSync()) {
                    ret.addIssue(CINE_WITH_EXIST.getDesc());
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                } else if (TableUtils.isACID(ret) && hmsMirrorConfig.isSync()) {
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
                    if (isBlank(hmsMirrorConfig.getTransfer().getTargetNamespace())) {
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
            rtn = buildTableSchema(rightSpec);
        }

        if (rtn && buildWhat.isTransferTable()) {
            // Build Transfer Spec.
            // When the source is ACID and we need to downgrade the dataset or Intermediate Storage is used.
            CopySpec transferSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.TRANSFER);
            // The Transfer Table should OWN the data, since we need to clean it up after the migration.
            transferSpec.setMakeNonTransactional(Boolean.TRUE);
            transferSpec.setMakeExternal(Boolean.TRUE);
            transferSpec.setTakeOwnership(Boolean.TRUE);

            transferSpec.setTableNamePrefix(hmsMirrorConfig.getTransfer().getTransferPrefix());
            transferSpec.setReplaceLocation(Boolean.TRUE);

            // Build transfer table.
            rtn = buildTableSchema(transferSpec);
        }

        // Build Shadow Spec (don't build when using commonStorage)
        // If acid and ma.isOn
        // if not downgrade

        if (buildWhat.isShadowTable()) {
            CopySpec shadowSpec = null;
            // If we built a transfer table, use it as the source for the shadow table.
            if (TableUtils.isACID(let) || !isBlank(hmsMirrorConfig.getTransfer().getIntermediateStorage())) {
                shadowSpec = new CopySpec(tableMirror, Environment.TRANSFER, Environment.SHADOW);
            } else {
                shadowSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.SHADOW);
            }
            shadowSpec.setUpgrade(Boolean.TRUE);
            shadowSpec.setMakeExternal(Boolean.TRUE);
            shadowSpec.setTakeOwnership(Boolean.FALSE);
            shadowSpec.setTableNamePrefix(hmsMirrorConfig.getTransfer().getShadowPrefix());

            rtn = buildTableSchema(shadowSpec);
        }

        return rtn;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        BuildWhat buildWhat = whatToBuild(config, tableMirror);

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


        // Manage Transfer Table.  Should only make this if the TRANSFER table is defined.
        if (buildWhat.isTransferTable()) {
            String transferCreateStmt = tableService.getCreateStatement(tableMirror, Environment.TRANSFER);
            if (!isBlank(transferCreateStmt)) {
                let.addSql(TableUtils.USE_DESC, useDb);
                // Drop any previous TRANSFER table, if it exists.
                if (!isBlank(tet.getName())) {
                    String transferDropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, tet.getName());
                    let.addSql(TableUtils.DROP_DESC, transferDropStmt);
                }
                let.addSql(TableUtils.CREATE_TRANSFER_DESC, transferCreateStmt);
            }
        }


        database = HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config);
        useDb = MessageFormat.format(MirrorConf.USE, database);
        ret.addSql(TableUtils.USE_DESC, useDb);

        // RIGHT SHADOW Table
        if (buildWhat.shadowTable) {
            String shadowCreateStmt = tableService.getCreateStatement(tableMirror, Environment.SHADOW);
            if (!isBlank(shadowCreateStmt)) {
                // Drop any previous SHADOW table, if it exists.
                String dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);
                // Create Shadow Table
                ret.addSql(TableUtils.CREATE_SHADOW_DESC, shadowCreateStmt);
                // Repair Partitions
                // TODO: Handle odd partitions.
                if (let.getPartitioned()) {
                    String shadowMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, set.getName());
                    ret.addSql(TableUtils.REPAIR_DESC, shadowMSCKStmt);
                }
            }
        }

        // RIGHT Final Table
        if (buildWhat.rightTable) {
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
                    if (!config.getCluster(Environment.RIGHT).isLegacyHive()
                            && config.isTransferOwnership() && let.getOwner() != null) {
                        String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, ret.getName(), let.getOwner());
                        ret.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
                    }
                    // Don't need this for final table..
                    // Unless we are using 'distcp' to copy the data.
                    // Partitioned, non-acid, w/ distcp.
                    if (let.getPartitioned() && config.getTransfer().getStorageMigration().isDistcp()
                        && !TableUtils.isACID(ret)) {
                                String rightMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, ret.getName());
                                ret.addSql(TableUtils.REPAIR_DESC, rightMSCKStmt);
                    }

                    break;
            }
        }
        rtn = Boolean.TRUE;
        return rtn;
    }


    @Override
    public BuildWhat whatToBuild(HmsMirrorConfig config, TableMirror tableMirror) {
        BuildWhat buildWhat = new BuildWhat();

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        // Build the RIGHT table definition.
        // TODO: Do we need to check for the existence of the RIGHT table?

            // Assumes Clusters are linked.
            // acid
                // intermediate
                    // transfer YES (downgrade to intermediate namespace)
                    // shadow YES (convert back from intermediate namespace)
                // no intermediate
                    // transfer YES (downgrade on source namespace)
                    // shadow YES (convert back from source namespace)
            // non-acid
                // intermediate
                    // transfer YES (transfer to intermediate namespace)
                    // shadow YES (convert back from intermediate namespace)
                // no intermediate
                    // transfer NO
                    // shadow YES (convert back from source namespace)
        if (TableUtils.isACID(let)) {
            buildWhat.transferTable = true;
            buildWhat.transferSql = true;
            buildWhat.shadowTable = true;
            buildWhat.shadowSql = true;
        } else {
            if (!isBlank(config.getTransfer().getIntermediateStorage())) {
                buildWhat.transferTable = true;
                buildWhat.transferSql = true;
                buildWhat.shadowTable = true;
                buildWhat.shadowSql = true;
            } else {
                buildWhat.shadowTable = true;
                buildWhat.shadowSql = true;
            }
        }

        // Build the RIGHT table definition.
        buildWhat.rightTable = true;

        return buildWhat;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        BuildWhat buildWhat = whatToBuild(config, tableMirror);

        // ================================
        try {
            rtn = buildOutDefinition(tableMirror);
        } catch (RequiredConfigurationException e) {
            let.addIssue("Failed to build out definition: " + e.getMessage());
            rtn = Boolean.FALSE;
        }

        if (rtn) {
            try {
                rtn = buildOutSql(tableMirror);
            } catch (MissingDataPointException e) {
                let.addIssue("Failed to build out SQL: " + e.getMessage());
                rtn = Boolean.FALSE;
            }
        }

        // Construct Transfer SQL
        if (rtn && buildWhat.isTransferSql()) {
            if (config.getCluster(Environment.LEFT).isLegacyHive()) {
                // We need to ensure that 'tez' is the execution engine.
                let.addSql(new Pair(TEZ_EXECUTION_DESC, SET_TEZ_AS_EXECUTION_ENGINE));
            }

            Pair cleanUp = new Pair("Post Migration Cleanup", "-- To be run AFTER final RIGHT SQL statements.");
            let.addCleanUpSql(cleanUp);

            String useLeftDb = MessageFormat.format(MirrorConf.USE, tableMirror.getParent().getName());
            Pair leftUsePair = new Pair(TableUtils.USE_DESC, useLeftDb);
            let.addCleanUpSql(leftUsePair);

            rtn = buildMigrationSql(tableMirror, Environment.LEFT, Environment.TRANSFER, Environment.TRANSFER);
            //tableMirror.transferSql(let, set, ret, config);
        }

        if (rtn && buildWhat.isShadowSql()) {
            rtn = buildMigrationSql(tableMirror, Environment.SHADOW, Environment.RIGHT, Environment.RIGHT);
        }

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

        return rtn;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }
}
