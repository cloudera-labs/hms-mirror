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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.concurrent.Callable;

public class GetTables implements Callable<ReturnStatus> {
    private static final Logger LOG = LogManager.getLogger(GetTables.class);

    private Config config = null;
    private DBMirror dbMirror = null;
    private boolean successful = Boolean.FALSE;

    public Config getConfig() {
        return config;
    }

    public DBMirror getDbMirror() {
        return dbMirror;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public GetTables(Config config, DBMirror dbMirror) {
        this.config = config;
        this.dbMirror = dbMirror;
    }

    @Override
    public ReturnStatus call() {
        ReturnStatus rtn = new ReturnStatus();
        LOG.debug("Getting tables for: " +dbMirror.getName());
        try {
            config.getCluster(Environment.LEFT).getTables(config, dbMirror);
            if (config.isSync()) {
                // Get the tables on the RIGHT side.  Used to determine if a table has been dropped on the LEFT
                // and later needs to be removed on the RIGHT.
                try {
                    config.getCluster(Environment.RIGHT).getTables(config, dbMirror);
                } catch (SQLException se) {
                    // OK, if the db doesn't exist yet.
                }
            }
            successful = Boolean.TRUE;
            rtn.setStatus(ReturnStatus.Status.SUCCESS);
        } catch (SQLException throwables) {
            successful = Boolean.FALSE;
            rtn.setStatus(ReturnStatus.Status.ERROR);
            rtn.setException(throwables);
        }
        return rtn;
    }
}
