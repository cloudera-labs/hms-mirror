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
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.HmsMirrorConfigUtil;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
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

import static com.cloudera.utils.hms.mirror.TablePropertyVars.EXTERNAL_TABLE_PURGE;
import static java.util.Objects.isNull;

@Component
@Slf4j
@Getter
public class ConvertLinkedDataStrategy extends DataStrategyBase implements DataStrategy {

    private ConfigService configService;
    private SchemaOnlyDataStrategy schemaOnlyDataStrategy;
    private TableService tableService;
//    private TranslatorService translatorService;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    public ConvertLinkedDataStrategy(ExecuteSessionService executeSessionService, TranslatorService translatorService) {
        this.executeSessionService = executeSessionService;
        this.translatorService = translatorService;
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

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);
        try {
            // If RIGHT doesn't exist, run SCHEMA_ONLY.
            if (isNull(ret)) {
                tableMirror.addIssue(Environment.RIGHT, "Table doesn't exist.  To transfer, run 'SCHEMA_ONLY'");
            } else {
                // Make sure table isn't an ACID table.
                if (TableUtils.isACID(let)) {
                    tableMirror.addIssue(Environment.LEFT, "ACID tables not eligible for this operation");
                } else if (tableMirror.isPartitioned(Environment.LEFT)) {
                    // We need to drop the RIGHT and RECREATE.
                    ret.addIssue("Table is partitioned.  Need to change data strategy to drop and recreate.");
                    String useDb = MessageFormat.format(MirrorConf.USE, HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config));
                    ret.addSql(MirrorConf.USE_DESC, useDb);

                    // Make sure the table is NOT set to purge.
                    if (TableUtils.isExternalPurge(ret)) {
                        String purgeSql = MessageFormat.format(MirrorConf.REMOVE_TABLE_PROP, ret.getName(), EXTERNAL_TABLE_PURGE);
                        ret.addSql(MirrorConf.REMOVE_TABLE_PROP_DESC, purgeSql);
                    }

                    String dropTable = MessageFormat.format(MirrorConf.DROP_TABLE, tableMirror.getName());
                    ret.addSql(MirrorConf.DROP_TABLE_DESC, dropTable);
                    tableMirror.setStrategy(DataStrategyEnum.SCHEMA_ONLY);
                    // Set False that it doesn't exist, which it won't, since we're dropping it.
                    ret.setExists(Boolean.FALSE);
                    rtn = schemaOnlyDataStrategy.execute(tableMirror);
                } else {
                    // - AVRO LOCATION
                    if (AVROCheck(tableMirror)) {
                        String useDb = MessageFormat.format(MirrorConf.USE, HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config));
                        ret.addSql(MirrorConf.USE_DESC, useDb);
                        // Look at the table definition and get.
                        // - LOCATION
                        String sourceLocation = TableUtils.getLocation(ret.getName(), ret.getDefinition());
                        String targetLocation = getTranslatorService().
                                translateTableLocation(tableMirror, sourceLocation, 1, null);
                        String alterLocSql = MessageFormat.format(MirrorConf.ALTER_TABLE_LOCATION, ret.getName(), targetLocation);
                        ret.addSql(MirrorConf.ALTER_TABLE_LOCATION_DESC, alterLocSql);
                        // TableUtils.updateTableLocation(ret, targetLocation)
                        // - Check Comments for "legacy.managed" setting.
                        //    - MirrorConf.HMS_MIRROR_LEGACY_MANAGED_FLAG (if so, set purge flag MirrorConf.EXTERNAL_TABLE_PURGE)
                        if (TableUtils.isHMSLegacyManaged(ret)) {
                            // ALTER TABLE x SET TBLPROPERTIES ('purge flag').
                            String purgeSql = MessageFormat.format(MirrorConf.ADD_TABLE_PROP, ret.getName(), EXTERNAL_TABLE_PURGE, "true");
                            ret.addSql(MirrorConf.ADD_TABLE_PROP_DESC, purgeSql);
                        }
                        rtn = Boolean.TRUE;

                        // Execute the RIGHT sql if config.execute.
                        if (rtn) {
                            rtn = tableService.runTableSql(tableMirror, Environment.RIGHT);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            log.error("Error executing ConvertLinkedDataStrategy", t);
            let.addIssue(t.getMessage());
            rtn = Boolean.FALSE;
        }

        return rtn;
    }

    @Autowired
    public void setSchemaOnlyDataStrategy(SchemaOnlyDataStrategy schemaOnlyDataStrategy) {
        this.schemaOnlyDataStrategy = schemaOnlyDataStrategy;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }

}
