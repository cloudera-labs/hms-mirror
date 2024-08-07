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

import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Getter
public class HybridDataStrategy extends DataStrategyBase implements DataStrategy {

    private ConfigService configService;

    private IntermediateDataStrategy intermediateDataStrategy;
    private SQLDataStrategy sqlDataStrategy;
    private ExportImportDataStrategy exportImportDataStrategy;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    public HybridDataStrategy(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        return null;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) throws MissingDataPointException {
        return null;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        // Need to look at table.  ACID tables go to doACID()
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        // Acid tables between legacy and non-legacy are forced to intermediate
        if (TableUtils.isACID(let) && configService.legacyMigration(config)) {
            tableMirror.setStrategy(DataStrategyEnum.ACID);
            if (config.getMigrateACID().isOn()) {
                rtn = intermediateDataStrategy.execute(tableMirror);
            } else {
                let.addIssue(TableUtils.ACID_NOT_ON);
                rtn = Boolean.FALSE;
            }
        } else {
            if (let.getPartitioned()) {
                if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit() &&
                        config.getHybrid().getExportImportPartitionLimit() > 0) {
                    // SQL
                    let.addIssue("The number of partitions: " + let.getPartitions().size()
                            + " exceeds the EXPORT_IMPORT "
                            + "partition limit (hybrid->exportImportPartitionLimit) of "
                            + config.getHybrid().getExportImportPartitionLimit() +
                            ".  Hence, the SQL method has been selected for the migration.");

                    tableMirror.setStrategy(DataStrategyEnum.SQL);
                    if (config.getTransfer().getIntermediateStorage() != null
                            || config.getTransfer().getTargetNamespace() != null) {
                        rtn = intermediateDataStrategy.execute(tableMirror);
                    } else {
                        rtn = sqlDataStrategy.execute(tableMirror);
                    }
                } else {
                    // EXPORT
                    tableMirror.setStrategy(DataStrategyEnum.EXPORT_IMPORT);
                    rtn = exportImportDataStrategy.execute(tableMirror);
                }
            } else {
                // EXPORT
                tableMirror.setStrategy(DataStrategyEnum.EXPORT_IMPORT);
                rtn = exportImportDataStrategy.execute(tableMirror);
            }
        }
        return rtn;

    }

    @Autowired
    public void setExportImportDataStrategy(ExportImportDataStrategy exportImportDataStrategy) {
        this.exportImportDataStrategy = exportImportDataStrategy;
    }

    @Autowired
    public void setIntermediateDataStrategy(IntermediateDataStrategy intermediateDataStrategy) {
        this.intermediateDataStrategy = intermediateDataStrategy;
    }

    @Autowired
    public void setSqlDataStrategy(SQLDataStrategy sqlDataStrategy) {
        this.sqlDataStrategy = sqlDataStrategy;
    }
}
