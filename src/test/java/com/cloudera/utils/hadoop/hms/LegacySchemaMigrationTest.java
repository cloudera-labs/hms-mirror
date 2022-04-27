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

import com.cloudera.utils.hadoop.hms.mirror.MessageCode;
import com.cloudera.utils.hadoop.hms.mirror.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.cloudera.utils.hadoop.hms.TestSQL.*;
import static org.junit.Assert.assertTrue;

public class LegacySchemaMigrationTest extends MirrorTestBase {

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

            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS, "acid_01", 2, TBL_INSERT, null, leftSql);
            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS, "acid_02", 6, TBL_INSERT, null, leftSql);
            build_n_populate(CREATE_EXTERNAL_TBL_PARTITIONED, "ext_part_01", null, TBL_INSERT_PARTITIONED, null, leftSql);
            build_n_populate(CREATE_EXTERNAL_TBL, "ext_part_02", null, TBL_INSERT, null, leftSql);

            Mirror cfgMirror = new Mirror();
            long rtn = cfgMirror.setupSql(args, leftSql, null);
            DataState.getInstance().setDataCreated(Boolean.TRUE);
        }
        return Boolean.TRUE;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        DataState.getInstance().setConfiguration(HDP2_CDP);
        dataSetup01();
    }

    @After
    public void tearDown() throws Exception {
        dataCleanup(Boolean.TRUE);
    }

    @Test
    public void test_acid_b_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "4",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY", "-db", DataState.getInstance().getWorking_db(),
                "-mao",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_common_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "COMMON", "-db", DataState.getInstance().getWorking_db(),
                "-sql",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_dbo() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-dbo",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);

        // We can do deeper testing on results here.
        // Conversion conversion = mirror.getConversion();

    }

    @Test
    public void test_dump_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "DUMP", "-db", DataState.getInstance().getWorking_db(),
                "-sql",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_linked_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "LINKED", "-db", DataState.getInstance().getWorking_db(),
                "-sql",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }


//    @Test
//    public void test_reset_right() {
//        reset();
//    }

    @Test
    public void test_so_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);

        // Testing for existing schemas.
        outputDir = outputDir + "/2";
        args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        rtn = 0;
        mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);

    }

    @Test
    public void test_so_rdl_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-rdl",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);

    }

    @Test
    public void test_so_rdl_w_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-rdl",
                "-wd", "/warehouse/managed",
                "-ewd", "/warehouse/external",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);

    }

    @Test
    public void test_so_ro_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-ro",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.RO_DB_DOESNT_EXIST.getLong();

        assertTrue("Return Code Failure", rtn == check);
    }

    @Test
    public void test_sp_limit() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-sp", "-1",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);

        // We can do deeper testing on results here.
        // Conversion conversion = mirror.getConversion();

    }

    @Test
    public void test_sql_leg() {
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