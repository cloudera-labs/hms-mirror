/*
 * Copyright (c) 2022. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hadoop.hms.datastrategy;

import com.cloudera.utils.hadoop.hms.DataState;
import com.cloudera.utils.hadoop.hms.Mirror;
import com.cloudera.utils.hadoop.hms.mirror.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.cloudera.utils.hadoop.hms.TestSQL.*;
import static com.cloudera.utils.hadoop.hms.TestSQL.TBL_INSERT;
import static org.junit.Assert.assertTrue;

public class AVROMigrationTest  extends MirrorTestBase {

    @Before
    public void init() throws Exception {
        super.init(HDP2_CDP);
//        DataState.getInstance().setConfiguration(HDP2_CDP);
        dataSetupAvro();
    }

    public Boolean dataSetupAvro() {
        if (!DataState.getInstance().isDataCreated("avro")) {
            String nameofCurrMethod = new Throwable()
                    .getStackTrace()[0]
                    .getMethodName();

            String outputDir = outputDirBase + nameofCurrMethod;

            String[] args = new String[]{"-d", "STORAGE_MIGRATION", "-smn", "s3a://something_not_relevant",
                    "-wd", "/hello", "-ewd", "/hello-ext",
                    "-db", DataState.getInstance().getWorking_db(), "-o", outputDir,
                    "-cfg", DataState.getInstance().getConfiguration()};
            args = toExecute(args, execArgs, Boolean.TRUE);

            List<Pair> leftSql = new ArrayList<Pair>();
            build_use_db(leftSql);

            List<String[]> dataset = null;
            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
                dataset = getDataset(4, 200, null);
            }
            build_n_populate(CREATE_AVRO_TBL_SHORT, TBL_INSERT, null, leftSql, new String[]{"avro_01", "hdfs://HDP50/user/dstreev/datasets/avro/test.avsc"});

            Mirror cfgMirror = new Mirror();
            long rtn = cfgMirror.setupSqlLeft(args, leftSql);

            DataState.getInstance().setDataCreated("avro", Boolean.TRUE);
        }
        return Boolean.TRUE;
    }

    @Test
    public void test_avro_01 () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY", "-db", DataState.getInstance().getWorking_db(),
                "-asm",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_avro_sql_02 () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-asm",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

}
