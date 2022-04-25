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

package com.cloudera.utils.hadoop.hms;

import com.cloudera.utils.hadoop.hms.mirror.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.cloudera.utils.hadoop.hms.TestSQL.*;
import static org.junit.Assert.assertTrue;

public class SQLDataMigrationTest extends MirrorTestBase {

    @AfterClass
    public static void tearDownClass() throws Exception {
        dataCleanup(Boolean.FALSE);
    }

    public Boolean dataSetup01() {
        if (!DataState.getInstance().isDataCreated()) {
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

            List<String[]> dataset = getDataset(2, 200, null);
            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS, "acid_01", 2, TBL_INSERT, dataset, leftSql);
            dataset = getDataset(2, 400, null);
            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS, "acid_02", 6, TBL_INSERT, dataset, leftSql);

            dataset = getDataset(2, 2000, 500);
            build_n_populate(CREATE_EXTERNAL_TBL_PARTITIONED, "ext_part_01", null, TBL_INSERT_PARTITIONED, dataset, leftSql);

            dataset = getDataset(2, 2000, null);
            build_n_populate(CREATE_EXTERNAL_TBL, "ext_part_02", null, TBL_INSERT, dataset, leftSql);

            Mirror cfgMirror = new Mirror();
            long rtn = cfgMirror.setupSql(args, leftSql, null);
            DataState.getInstance().setDataCreated(Boolean.TRUE);
        }
        return Boolean.TRUE;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        DataState.getInstance().setConfiguration(CDP_CDP);
        dataSetup01();
    }

    @After
    public void tearDown() throws Exception {
        dataCleanup(Boolean.TRUE);
    }

    @Test
    public void test_acid_sql() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_all() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-ma",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_cs() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-cs", common_storage,
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_da_r_all_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-ma", "-da", "-r",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_cs_w() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-cs", common_storage,
                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_da() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-da",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_da_cs() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-cs", common_storage, "-da", "-o", outputDir,
                "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_w_all() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-ma",
                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_sql() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-sql",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

}