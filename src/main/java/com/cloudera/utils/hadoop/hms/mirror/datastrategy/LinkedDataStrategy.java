/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hadoop.hms.mirror.datastrategy;

import com.cloudera.utils.hadoop.hms.mirror.CopySpec;
import com.cloudera.utils.hadoop.hms.mirror.CreateStrategy;
import com.cloudera.utils.hadoop.hms.mirror.Environment;
import com.cloudera.utils.hadoop.hms.mirror.EnvironmentTable;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.SCHEMA_EXISTS_NO_ACTION;

public class LinkedDataStrategy extends DataStrategyBase implements DataStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(LinkedDataStrategy.class);
    @Override
    public Boolean execute() {
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable tet = tableMirror.getEnvironmentTable(Environment.TRANSFER);
        EnvironmentTable set = tableMirror.getEnvironmentTable(Environment.SHADOW);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        if (TableUtils.isACID(let)) {
            tableMirror.addIssue(Environment.LEFT, "You can't 'LINK' ACID tables.");
            rtn = Boolean.FALSE;
        } else {
            rtn = buildOutDefinition();//tblMirror.buildoutLINKEDDefinition(config, dbMirror);
        }

        if (rtn) {
            rtn = buildOutSql();//tblMirror.buildoutLINKEDSql(config, dbMirror);
        }

        // Execute the RIGHT sql if config.execute.
        if (rtn) {
            rtn = config.getCluster(Environment.RIGHT).runTableSql(tableMirror);
        }

        return rtn;

    }

    @Override
    public Boolean buildOutDefinition() {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + tableMirror.getName() + " buildout LINKED Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        // Can't LINK ACID tables.
        if (TableUtils.isHiveNative(let) && !TableUtils.isACID(let)) {
            // Swap out the namespace of the LEFT with the RIGHT.
            copySpec.setReplaceLocation(Boolean.FALSE);
            if (config.convertManaged())
                copySpec.setUpgrade(Boolean.TRUE);
            // LINKED doesn't own the data.
            copySpec.setTakeOwnership(Boolean.FALSE);

            if (config.isSync()) {
                // We assume that the 'definitions' are only there is the
                //     table exists.
                if (!let.getExists() && ret.getExists()) {
                    // If left is empty and right is not, DROP RIGHT.
                    ret.addIssue("Schema doesn't exist in 'source'.  Will be DROPPED.");
                    ret.setCreateStrategy(CreateStrategy.DROP);
                } else if (let.getExists() && !ret.getExists()) {
                    // If left is defined and right is not, CREATE RIGHT.
                    ret.addIssue("Schema missing, will be CREATED");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                } else if (let.getExists() && ret.getExists()) {
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
                if (ret.getExists()) {
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
            rtn = tableMirror.buildTableSchema(copySpec);
        } else {
            let.addIssue("Can't LINK ACID tables");
            ret.setCreateStrategy(CreateStrategy.NOTHING);
        }
        return rtn;
    }

    @Override
    public Boolean buildOutSql() {
        DataStrategy dsSO = DataStrategyEnum.SCHEMA_ONLY.getDataStrategy();
        dsSO.setTableMirror(tableMirror);
        dsSO.setDBMirror(dbMirror);
        dsSO.setConfig(config);
        return dsSO.execute();
    }
}
