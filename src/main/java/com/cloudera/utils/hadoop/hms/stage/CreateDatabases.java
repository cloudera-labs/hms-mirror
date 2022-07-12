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

import com.cloudera.utils.hadoop.hms.mirror.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

public class CreateDatabases implements Callable<ReturnStatus> {
    private static final Logger LOG = LogManager.getLogger(CreateDatabases.class);

    private Config config = null;
    private Conversion conversion = null;

    // Flag use to determine is creating transition db in lower cluster
    // or target db in upper cluster.
    private final boolean successful = Boolean.FALSE;

    public boolean isSuccessful() {
        return successful;
    }

    public CreateDatabases(Config config, Conversion conversion) {
        this.config = config;
        this.conversion = conversion;
    }

    @Override
    public ReturnStatus call() {
        ReturnStatus rtn = new ReturnStatus();
        LOG.debug("Create Databases");
        for (String database : config.getDatabases()) {
            DBMirror dbMirror = conversion.getDatabase(database);

            dbMirror.buildDBStatements(config);

            if (config.getCluster(Environment.LEFT).runDatabaseSql(dbMirror)) {
                rtn.setStatus(ReturnStatus.Status.SUCCESS);
            }

            if (config.getCluster(Environment.RIGHT).runDatabaseSql(dbMirror)) {
                rtn.setStatus(ReturnStatus.Status.SUCCESS);
            }
        }
        rtn.setStatus(ReturnStatus.Status.SUCCESS);
        return rtn;
    }
}
