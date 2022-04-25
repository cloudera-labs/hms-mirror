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

public class StorageMigrationTest extends MirrorTestBase {

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

            String[] args = new String[]{"-d", "STORAGE_MIGRATION", "-smn", "s3a://my_dump_bucket",
                    "-wd", "/hello", "-ewd", "/hello-ext",
                    "-db", DataState.getInstance().getWorking_db(), "-o", outputDir,
                    "-cfg", DataState.getInstance().getConfiguration()};
            args = toExecute(args, execArgs, Boolean.TRUE);

            List<Pair> leftSql = new ArrayList<Pair>();
            build_use_db(leftSql);

            List<String[]> dataset = getDataset(2, 200, null);
            build_n_populate(CREATE_ACID_TBL, "acid_01", null, TBL_INSERT, dataset, leftSql);
            dataset = getDataset(2, 400, null);
            build_n_populate(CREATE_ACID_TBL_N_BUCKETS, "acid_02", 6, TBL_INSERT, dataset, leftSql);

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
        DataState.getInstance().setConfiguration(CDP);
        dataSetup01();
    }

    @After
    public void tearDown() throws Exception {
        dataCleanup(Boolean.TRUE);
    }

    @Test
    public void test_datasetup() {
        System.out.println("Data setup.");
    }

    @Test
    public void test_storage_migration_02() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-d", "STORAGE_MIGRATION",
                "-smn", common_storage,
                "-wd", "/warehouse/managed_tables", "-ewd", "/warehouse/external_tables",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + 0l, rtn == 0);
    }

    @Test
    public void test_storage_migration_03() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-d", "STORAGE_MIGRATION",
                "-smn", common_storage,
                "-ma",
                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
    }

    @Test
    public void test_storage_migration_04() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-d", "STORAGE_MIGRATION",
                "-smn", common_storage,
                "-ma", "6", "-da",
                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
    }

    @Test
    public void test_storage_migration_05() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-d", "STORAGE_MIGRATION",
                "-smn", common_storage,
                "-ma", "4",
                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
    }

}