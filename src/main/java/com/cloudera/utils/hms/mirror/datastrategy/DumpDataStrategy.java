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
import com.cloudera.utils.hms.mirror.service.TranslatorService;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

@Component
@Slf4j
@Getter
public class DumpDataStrategy extends DataStrategyBase implements DataStrategy {

    private TableService tableService;
    private TranslatorService translatorService;

    public DumpDataStrategy(HmsMirrorCfgService hmsMirrorCfgService) {
        this.hmsMirrorCfgService = hmsMirrorCfgService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        log.debug("Table: {} buildout DUMP Definition", tableMirror.getName());
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

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
    public Boolean buildOutSql(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

        log.debug("Table: {} buildout DUMP SQL", tableMirror.getName());

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        let.getSql().clear();
        useDb = MessageFormat.format(MirrorConf.USE, tableMirror.getParent().getName());
        let.addSql(TableUtils.USE_DESC, useDb);

        createTbl = tableService.getCreateStatement(tableMirror, Environment.LEFT);
        let.addSql(TableUtils.CREATE_DESC, createTbl);
        if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()
                && getHmsMirrorCfgService().getHmsMirrorConfig().isTransferOwnership() && let.getOwner() != null) {
            String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, let.getName(), let.getOwner());
            let.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
        }

        // If partitioned, !ACID, repair
        if (let.getPartitioned() && !TableUtils.isACID(let)) {
            if (getHmsMirrorCfgService().getHmsMirrorConfig().isEvaluatePartitionLocation()) {
                String tableParts = getTranslatorService().buildPartitionAddStatement(let);
                String addPartSql = MessageFormat.format(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION, let.getName(), tableParts);
                let.addSql(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION_DESC, addPartSql);
            } else if (getHmsMirrorCfgService().getHmsMirrorConfig().getCluster(Environment.LEFT).getPartitionDiscovery().isInitMSCK()) {
                String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, let.getName());
                if (getHmsMirrorCfgService().getHmsMirrorConfig().getTransfer().getStorageMigration().isDistcp()) {
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
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;

        rtn = buildOutDefinition(tableMirror);
        if (rtn) {
            rtn = buildOutSql(tableMirror);
        }
        return rtn;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }

    @Autowired
    public void setTranslatorService(TranslatorService translatorService) {
        this.translatorService = translatorService;
    }
}
