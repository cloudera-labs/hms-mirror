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

import static com.cloudera.utils.hms.mirror.MessageCode.SCHEMA_EXISTS_NO_ACTION;

@Component
@Slf4j
public class LinkedDataStrategy extends DataStrategyBase implements DataStrategy {

    @Getter
    private SchemaOnlyDataStrategy schemaOnlyDataStrategy;

    @Getter
    private TableService tableService;

    public LinkedDataStrategy(ConfigService configService) {
        this.configService = configService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        log.debug("Table: " + tableMirror.getName() + " buildout LINKED Definition");
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
            if (getConfigService().getConfig().convertManaged())
                copySpec.setUpgrade(Boolean.TRUE);
            // LINKED doesn't own the data.
            copySpec.setTakeOwnership(Boolean.FALSE);

            if (getConfigService().getConfig().isSync()) {
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
            rtn = tableService.buildTableSchema(copySpec);
        } else {
            let.addIssue("Can't LINK ACID tables");
            ret.setCreateStrategy(CreateStrategy.NOTHING);
        }
        return rtn;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) {
        return schemaOnlyDataStrategy.execute(tableMirror);
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable tet = getEnvironmentTable(Environment.TRANSFER, tableMirror);
        EnvironmentTable set = getEnvironmentTable(Environment.SHADOW, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        if (TableUtils.isACID(let)) {
            tableMirror.addIssue(Environment.LEFT, "You can't 'LINK' ACID tables.");
            rtn = Boolean.FALSE;
        } else {
            rtn = buildOutDefinition(tableMirror);//tblMirror.buildoutLINKEDDefinition(config, dbMirror);
        }

        if (rtn) {
            rtn = buildOutSql(tableMirror);//tblMirror.buildoutLINKEDSql(config, dbMirror);
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
