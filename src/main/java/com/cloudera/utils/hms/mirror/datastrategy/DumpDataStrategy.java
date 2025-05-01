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
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.HmsMirrorConfigUtil;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.StatsCalculatorService;
import com.cloudera.utils.hms.mirror.service.TableService;
import com.cloudera.utils.hms.mirror.service.TranslatorService;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Slf4j
@Getter
public class DumpDataStrategy extends DataStrategyBase {

    private final TableService tableService;

    public DumpDataStrategy(StatsCalculatorService statsCalculatorService,
                            ExecuteSessionService executeSessionService,
                            TranslatorService translatorService,
                            TableService tableService) {
        super(statsCalculatorService, executeSessionService, translatorService);
        this.tableService = tableService;
    }

    @Override
    public Boolean buildOutDefinition( TableMirror tableMirror) {
        log.debug("Table: {} buildout DUMP Definition", tableMirror.getName());
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT, tableMirror);
        // Standardize the LEFT def.
        // Remove DB from CREATE
        TableUtils.stripDatabase(let.getName(), let.getDefinition());

        // If not legacy, remove location from ACID tables.
        if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive() &&
                TableUtils.isACID(let)) {
            TableUtils.stripLocation(let.getName(), let.getDefinition());
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        log.debug("Table: {} buildout DUMP SQL", tableMirror.getName());

        String useDb = null;
//        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        let.getSql().clear();
        String database = HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config);
        useDb = MessageFormat.format(MirrorConf.USE, database);
        let.addSql(TableUtils.USE_DESC, useDb);

        createTbl = tableService.getCreateStatement(tableMirror, Environment.LEFT);
        let.addSql(TableUtils.CREATE_DESC, createTbl);
        if (!config.getCluster(Environment.LEFT).isLegacyHive()
                && config.getOwnershipTransfer().isTable() && let.getOwner() != null) {
            String ownerSql = MessageFormat.format(MirrorConf.SET_TABLE_OWNER, let.getName(), let.getOwner());
            let.addSql(MirrorConf.SET_TABLE_OWNER_DESC, ownerSql);
        }

        // If partitioned, !ACID, repair
        if (let.getPartitioned() && !TableUtils.isACID(let)) {
            if (config.loadMetadataDetails()) {
                String tableParts = getTranslatorService().buildPartitionAddStatement(let);
                // This will be empty when there's no data and we need to handle that.
                if (!isBlank(tableParts)) {
                    String addPartSql = MessageFormat.format(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION, let.getName(), tableParts);
                    let.addSql(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION_DESC, addPartSql);
                }
            } else if (config.getCluster(Environment.LEFT).getPartitionDiscovery().isInitMSCK()) {
                String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, let.getName());
                if (config.getTransfer().getStorageMigration().isDistcp()) {
                    let.addCleanUpSql(TableUtils.REPAIR_DESC, msckStmt);
                } else {
                    let.addSql(TableUtils.REPAIR_DESC, msckStmt);
                }
            }
        }

        rtn = Boolean.TRUE;

        return rtn;
    }

    @Override
    public Boolean build(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;

        rtn = buildOutDefinition(tableMirror);
        if (rtn) {
            try {
                rtn = buildOutSql(tableMirror);
            } catch (MissingDataPointException e) {
                EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
                log.error("Table: {} Missing Data Point: {}", let.getName(), e.getMessage());
                let.addError("Failed to build out SQL: " + e.getMessage());
                rtn = Boolean.FALSE;
            }
        }
        return rtn;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        // No Action to perform.
        return Boolean.TRUE;
    }

}
