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
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.TableService;
import com.cloudera.utils.hms.mirror.service.TranslatorService;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.cloudera.utils.hms.mirror.MessageCode.SCHEMA_EXISTS_NO_ACTION;
import static com.cloudera.utils.hms.mirror.MessageCode.TABLE_ISSUE;

@Component
@Slf4j
@Getter
public class LinkedDataStrategy extends DataStrategyBase implements DataStrategy {

    private SchemaOnlyDataStrategy schemaOnlyDataStrategy;
    private TableService tableService;

    public LinkedDataStrategy(ExecuteSessionService executeSessionService, TranslatorService translatorService) {
        this.executeSessionService = executeSessionService;
        this.translatorService = translatorService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) throws RequiredConfigurationException {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        log.debug("Table: {} buildout LINKED Definition", tableMirror.getName());
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT, tableMirror);
        ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        copySpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);
        // Can't LINK ACID tables.
        if (TableUtils.isHiveNative(let) && !TableUtils.isACID(let)) {
            // Swap out the namespace of the LEFT with the RIGHT.
            copySpec.setReplaceLocation(Boolean.FALSE);
            if (hmsMirrorConfig.convertManaged())
                copySpec.setUpgrade(Boolean.TRUE);
            // LINKED doesn't own the data.
            copySpec.setTakeOwnership(Boolean.FALSE);

            if (hmsMirrorConfig.isSync()) {
                // We assume that the 'definitions' are only there is the
                //     table exists.
                if (!let.isExists() && ret.isExists()) {
                    // If left is empty and right is not, DROP RIGHT.
                    ret.addIssue("Schema doesn't exist in 'source'.  Will be DROPPED.");
                    ret.setCreateStrategy(CreateStrategy.DROP);
                } else if (let.isExists() && !ret.isExists()) {
                    // If left is defined and right is not, CREATE RIGHT.
                    ret.addIssue("Schema missing, will be CREATED");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                } else if (let.isExists() && ret.isExists()) {
                    // If left and right, check schema change and replace if necessary.
                    // Compare Schemas.
                    if (tableMirror.schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                        ret.addIssue(SCHEMA_EXISTS_NO_ACTION.getDesc());
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                        log.error(TABLE_ISSUE.getDesc(), tableMirror.getParent().getName(), tableMirror.getName(),
                                SCHEMA_EXISTS_NO_ACTION.getDesc());
                    } else {
                        if (TableUtils.isExternalPurge(ret)) {
                            ret.addIssue("Schema exists AND DOESN'T match.  But the 'RIGHT' table is has a PURGE option set. " +
                                    "We can NOT safely replace the table without compromising the data. No action will be taken.");
                            ret.setCreateStrategy(CreateStrategy.LEAVE);
                            return Boolean.FALSE;
                        } else {
                            ret.addIssue("Schema exists AND DOESN'T match.  It will be REPLACED (DROPPED and RECREATED).");
                            ret.setCreateStrategy(CreateStrategy.REPLACE);
                        }
                    }
                }
                copySpec.setTakeOwnership(Boolean.FALSE);
            } else {
                if (ret.isExists()) {
                    // Already exists, no action.
                    ret.addIssue("Schema exists already, no action. If you wish to rebuild the schema, " +
                            "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                    return Boolean.FALSE;
                } else {
                    ret.addIssue("Schema will be created");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                }
            }
            // Rebuild Target from Source.
            rtn = buildTableSchema(copySpec);
        } else {
            let.addIssue("Can't LINK ACID tables");
            ret.setCreateStrategy(CreateStrategy.NOTHING);
        }
        return rtn;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) throws MissingDataPointException {
        // Reuse the SchemaOnlyDataStrategy to build out the DDL SQL for the LINKED table.
        return schemaOnlyDataStrategy.buildOutSql(tableMirror);
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);

        if (TableUtils.isACID(let)) {
            tableMirror.addIssue(Environment.LEFT, "You can't 'LINK' ACID tables.");
            rtn = Boolean.FALSE;
        } else {
            try {
                rtn = buildOutDefinition(tableMirror);//tblMirror.buildoutLINKEDDefinition(config, dbMirror);
            } catch (RequiredConfigurationException e) {
                let.addIssue("Failed to build out definition: " + e.getMessage());
                rtn = Boolean.FALSE;
            }
        }

        if (rtn) {
            try {
                rtn = buildOutSql(tableMirror);//tblMirror.buildoutLINKEDSql(config, dbMirror);
            } catch (MissingDataPointException e) {
                let.addIssue("Failed to build out SQL: " + e.getMessage());
                rtn = Boolean.FALSE;
            }
        }

        // Execute the RIGHT sql if config.execute.
        if (rtn) {
            rtn = tableService.runTableSql(tableMirror, Environment.RIGHT);
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
