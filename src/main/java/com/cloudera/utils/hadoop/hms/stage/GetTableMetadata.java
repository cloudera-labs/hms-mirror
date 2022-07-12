/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms.stage;

import com.cloudera.utils.hadoop.hms.mirror.Config;
import com.cloudera.utils.hadoop.hms.mirror.DBMirror;
import com.cloudera.utils.hadoop.hms.mirror.Environment;
import com.cloudera.utils.hadoop.hms.mirror.TableMirror;
import com.cloudera.utils.hadoop.hms.mirror.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.concurrent.Callable;

public class GetTableMetadata implements Callable<ReturnStatus> {
    private static final Logger LOG = LogManager.getLogger(GetTableMetadata.class);

    private Config config = null;
    private DBMirror dbMirror = null;
    private TableMirror tblMirror = null;
    private boolean successful = Boolean.TRUE;

    public Config getConfig() {
        return config;
    }

    public DBMirror getDbMirror() {
        return dbMirror;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public GetTableMetadata(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        this.config = config;
        this.dbMirror = dbMirror;
        this.tblMirror = tblMirror;
    }

    @Override
    public ReturnStatus call() {
        return doit();
    }

    public ReturnStatus doit() {
        ReturnStatus rtn = new ReturnStatus();
        LOG.debug("Getting table definition for: " + dbMirror.getName() + "." + tblMirror.getName());
        try {
            config.getCluster(Environment.LEFT).getTableDefinition(config, dbMirror.getName(), tblMirror);
            switch (config.getDataStrategy()) {
                case DUMP:
                    successful = Boolean.TRUE;
                    break;
                default:
                    config.getCluster(Environment.RIGHT).getTableDefinition(config, config.getResolvedDB(dbMirror.getName()), tblMirror);
            }
        } catch (SQLException throwables) {
            successful = Boolean.FALSE;
        }
        if (successful) {
            rtn.setStatus(ReturnStatus.Status.SUCCESS);
        } else {
            rtn.setStatus(ReturnStatus.Status.ERROR);
        }
        LOG.debug("Completed table definition for: " + dbMirror.getName() + "." + tblMirror.getName());
        return rtn;
    }
}
