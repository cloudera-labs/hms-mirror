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
import com.cloudera.utils.hms.mirror.service.StatsCalculatorService;
import com.cloudera.utils.hms.mirror.service.TranslatorService;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Slf4j
@Getter
public class HybridDataStrategy extends DataStrategyBase {

    private final ConfigService configService;

    private final IntermediateDataStrategy intermediateDataStrategy;
    private final SQLDataStrategy sqlDataStrategy;
    private final ExportImportDataStrategy exportImportDataStrategy;

    public HybridDataStrategy(StatsCalculatorService statsCalculatorService,
                              ExecuteSessionService executeSessionService,
                              TranslatorService translatorService,
                              ConfigService configService,
                              IntermediateDataStrategy intermediateDataStrategy,
                              SQLDataStrategy sqlDataStrategy,
                              ExportImportDataStrategy exportImportDataStrategy) {
        super(statsCalculatorService, executeSessionService, translatorService);
        this.configService = configService;
        this.intermediateDataStrategy = intermediateDataStrategy;
        this.sqlDataStrategy = sqlDataStrategy;
        this.exportImportDataStrategy = exportImportDataStrategy;
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
    public Boolean build(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        // Need to look at table.  ACID tables go to doACID()
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        // Acid tables between legacy and non-legacy are forced to intermediate
        if (TableUtils.isACID(let) && configService.legacyMigration(config)) {
            tableMirror.setStrategy(DataStrategyEnum.ACID);
            if (config.getMigrateACID().isOn()) {
                rtn = intermediateDataStrategy.build(tableMirror);
            } else {
                let.addError(TableUtils.ACID_NOT_ON);
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
                    if (!isBlank(config.getTransfer().getIntermediateStorage())
                            || !isBlank(config.getTransfer().getTargetNamespace())) {
                        rtn = intermediateDataStrategy.build(tableMirror);
                    } else {
                        rtn = sqlDataStrategy.build(tableMirror);
                    }
                } else {
                    // EXPORT
                    tableMirror.setStrategy(DataStrategyEnum.EXPORT_IMPORT);
                    rtn = exportImportDataStrategy.build(tableMirror);
                }
            } else {
                // EXPORT
                tableMirror.setStrategy(DataStrategyEnum.EXPORT_IMPORT);
                rtn = exportImportDataStrategy.build(tableMirror);
            }
        }
        return rtn;

    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        if (TableUtils.isACID(let) && configService.legacyMigration(config)) {
            tableMirror.setStrategy(DataStrategyEnum.ACID);
            if (config.getMigrateACID().isOn()) {
                rtn = intermediateDataStrategy.execute(tableMirror);
            } else {
                rtn = Boolean.FALSE;
            }
        } else {
            if (let.getPartitioned()) {
                if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit() &&
                        config.getHybrid().getExportImportPartitionLimit() > 0) {
                    if (!isBlank(config.getTransfer().getIntermediateStorage())
                            || !isBlank(config.getTransfer().getTargetNamespace())) {
                        rtn = intermediateDataStrategy.execute(tableMirror);
                    } else {
                        rtn = sqlDataStrategy.execute(tableMirror);
                    }
                } else {
                    // EXPORT
                    rtn = exportImportDataStrategy.execute(tableMirror);
                }
            } else {
                // EXPORT
                rtn = exportImportDataStrategy.execute(tableMirror);
            }
        }
        return rtn;
    }

}
